# Builtin modules
from __future__ import annotations
import json, weakref
from time import monotonic
from typing import Any, Union, List, Dict, Tuple, Optional, cast
# Third party modules
import fsPacker
from fsLogger import Logger
from fsSignal import HardSignal, T_Signal, KillSignal
# Local modules
from .exceptions import InitializationError, MessageError, RequestError, ResponseError, SocketError
from .utils import iterSplit, Headers
from .clientSocket import HTTPClientSocket, StringClientSocket, FSPackerClientSocket
from .abcs import T_Request, T_Client
# Program
class Client(T_Client):
	max_bulk_request = 0xFF
	def __init__(self, protocol:str, target:Union[str, Tuple[str, int], Tuple[str, int, int, int]], connectionTimeout:int=15,
	transferTimeout:int=320, retryCount:int=10, retryDelay:int=5, ssl:bool=False, sslHostname:Optional[str]=None,
	httpHost:Optional[str]=None, extraHttpHeaders:Dict[str, str]={}, httpMethod:str="POST", httpPath:str="/",
	disableCompression:bool=False, log:Optional[Logger]=None, signal:Optional[T_Signal]=None):
		self.protocol           = protocol
		self.connectionTimeout  = connectionTimeout
		self.transferTimeout    = transferTimeout
		self.retryCount         = retryCount
		self.retryDelay         = retryDelay
		self.ssl                = ssl
		self.sslHostname        = sslHostname
		self.httpHost           = httpHost
		self.extraHttpHeaders   = extraHttpHeaders
		self.httpMethod         = httpMethod
		self.httpPath           = httpPath
		self.disableCompression = disableCompression
		self.log                = log or Logger("RPCClient")
		self.signal             = signal or HardSignal
		self.id                 = 0
		self.requests           = weakref.WeakValueDictionary()
		if ":" not in self.protocol:
			raise InitializationError("Invalid protocol")
		protocolParts = self.protocol.split(":")
		if len(protocolParts) != 3:
			raise InitializationError("Invalid protocol")
		self.socketProtocol, self.messageProtocol, self.requestProtocol = protocolParts
		if self.socketProtocol == "IPC":
			self.target = cast(str, target)
			if not isinstance(self.target, str):
				raise InitializationError("Target must be path")
		elif self.socketProtocol == "TCPv4":
			self.target = cast(Tuple[str, int], target)
			if not isinstance(self.target, (tuple, list)) or len(self.target) != 2 or \
			not isinstance(self.target[0], str) or not isinstance(self.target[1], int):
				raise InitializationError("Target must be address and port in a list\tuple")
		elif self.socketProtocol == "TCPv6":
			self.target = cast(Tuple[str, int, int, int], target)
			if  not isinstance(self.target, (tuple, list)) or len(self.target) != 4 or \
			not isinstance(self.target[0], str) or not isinstance(self.target[1], int) or \
			not isinstance(self.target[2], int) or not isinstance(self.target[3], int):
				raise InitializationError("Target must be address, port, flow info and scope id in a list\tuple")
		else:
			raise InitializationError("Unsupported socket protocol")
		if self.messageProtocol not in ["HTTP", "STR", "FSP"]:
			raise InitializationError("Unsupported message protocol")
		if self.requestProtocol not in ["JSONRPC-2", "JSONRPC-P", "FSP"]:
			raise InitializationError("Unsupported request protocol")
		if self.log.isFiltered("TRACE"):
			self.log.debug(
				"Initialized  [sockProt: {}][msgProt: {}][reqProt: {}][target: {}][SSL: {}]",
				self.socketProtocol, self.messageProtocol, self.requestProtocol, self.target, self.ssl
			)
		elif self.log.isFiltered("DEBUG"):
			self.log.debug("Initialized")
	def __del__(self) -> None:
		self.close()
	def __enter__(self) -> Any:
		return self.clone()
	def __exit__(self, type:Any, value:Any, traceback:Any) -> None:
		self.close()
	def __getstate__(self) -> Dict[str, Any]:
		return {
			"target":            self.target,
			"protocol":          self.protocol,
			"connectionTimeout": self.connectionTimeout,
			"transferTimeout":   self.transferTimeout,
			"retryCount":        self.retryCount,
			"retryDelay":        self.retryDelay,
			"ssl":               self.ssl,
			"sslHostname":       self.sslHostname,
			"httpHost":          self.httpHost,
			"extraHttpHeaders":  self.extraHttpHeaders,
			"httpMethod":        self.httpMethod,
			"httpPath":          self.httpPath,
			"disableCompression":self.disableCompression,
			"log":               self.log,
			"signal":            self.signal,
		}
	def __setstate__(self, states:Dict[str, Any]) -> None:
		self.target             = states["target"]
		self.protocol           = states["protocol"]
		self.connectionTimeout  = states["connectionTimeout"]
		self.transferTimeout    = states["transferTimeout"]
		self.retryCount         = states["retryCount"]
		self.retryDelay         = states["retryDelay"]
		self.ssl                = states["ssl"]
		self.sslHostname        = states["sslHostname"]
		self.httpHost           = states["httpHost"]
		self.extraHttpHeaders   = states["extraHttpHeaders"]
		self.httpMethod         = states["httpMethod"]
		self.httpPath           = states["httpPath"]
		self.disableCompression = states["disableCompression"]
		self.log                = states["log"]
		self.signal             = states["signal"]
		self.id                 = 0
		self.requests           = weakref.WeakValueDictionary()
	def _connect(self, sendOlderRequests:bool=True) -> None:
		self.socket.connect()
		self.log.info("Connected")
		if sendOlderRequests:
			objs:List[T_Request] = list(filter(lambda x: not x.isDone(), self.requests.values()))
			if objs:
				objs.sort(key=lambda x: x._requestTime)
				chunks = [ obj._dumps() for obj in objs ]
				if chunks:
					self.log.info("Sending {} previous requests", len(chunks))
					if len(chunks) > 1:
						for chunk in iterSplit(chunks, self.max_bulk_request):
							self._sendRequest(chunk)
					else:
						self._sendRequest(chunks[0])
	def _sendRequest(self, data:Any) -> None:
		payload:bytes
		if isinstance(self.socket, HTTPClientSocket):
			extraHttpHeaders:Dict[str, str]={}
			extraHttpHeaders["host"] = self.httpHost or self.target[0]
			if self.requestProtocol in ["JSONRPC-2", "JSONRPC-P"]:
				extraHttpHeaders["Content-Type"] = "application/json;charset=utf-8"
				payload = json.dumps(data).encode('utf8')
			elif self.requestProtocol == "FSP":
				extraHttpHeaders["Content-Type"] = "application/fspacker"
				payload = fsPacker.dumps(data)
			else:
				raise RuntimeError
			self.socket.send(payload, extraHttpHeaders)
		elif isinstance(self.socket, FSPackerClientSocket):
			if self.requestProtocol in ["JSONRPC-2", "JSONRPC-P"]:
				payload = json.dumps(data).encode('utf8')
			elif self.requestProtocol == "FSP":
				payload = fsPacker.dumps(data)
			else:
				raise RuntimeError
			self.socket.send(payload)
		elif isinstance(self.socket, StringClientSocket):
			if self.requestProtocol in ["JSONRPC-2", "JSONRPC-P"]:
				payload = json.dumps(data).encode('utf8')
			elif self.requestProtocol == "FSP":
				payload = fsPacker.dumps(data).hex().encode('utf8')
			else:
				raise RuntimeError
			self.socket.send(payload)
		else:
			raise RuntimeError
	def _createSocket(self) -> None:
		if not hasattr(self, "socket"):
			if self.messageProtocol == "HTTP":
				self.socket = HTTPClientSocket(
					client             = self,
					protocol           = self.socketProtocol,
					target             = self.target,
					connectionTimeout  = self.connectionTimeout,
					transferTimeout    = self.transferTimeout,
					ssl                = self.ssl,
					sslHostname        = self.sslHostname,
					extraHeaders       = self.extraHttpHeaders,
					httpMethod         = self.httpMethod,
					path               = self.httpPath,
					disableCompression = self.disableCompression,
				)
			elif self.messageProtocol == "STR":
				self.socket = StringClientSocket(
					client            = self,
					protocol          = self.socketProtocol,
					target            = self.target,
					connectionTimeout = self.connectionTimeout,
					transferTimeout   = self.transferTimeout,
					ssl               = False,
					sslHostname       = None,
				)
			elif self.messageProtocol == "FSP":
				self.socket = FSPackerClientSocket(
					client            = self,
					protocol          = self.socketProtocol,
					target            = self.target,
					connectionTimeout = self.connectionTimeout,
					transferTimeout   = self.transferTimeout,
					ssl               = False,
					sslHostname       = None,
				)
			else:
				raise RuntimeError
	def _get(self, id:Any) -> None:
		def wh() -> bool:
			return not self.requests[id].isDone()
		if id not in self.requests:
			raise ResponseError("Unknown request ID: {}".format(id))
		c = self.retryCount
		while True:
			try:
				self.signal.check()
				if hasattr(self, "socket") and not self.socket.isAlive():
					del self.socket
				if not hasattr(self, "socket"):
					self._createSocket()
					self._connect()
				self.socket.loop(wh)
				break
			except KillSignal:
				raise
			except SocketError:
				self.log.warn("Error while getting response")
				if c > 0:
					self.signal.sleep(self.retryDelay)
					c -= 1
					del self.socket
					continue
				raise
	def _parseResponse(self, payload:bytes, headers:Optional[Headers]=None, charset:str="utf8") -> None:
		if self.requestProtocol in ["JSONRPC-2", "JSONRPC-P"]:
			try:
				data = json.loads(payload.decode(charset))
			except:
				raise MessageError("Invalid payload")
			if not isinstance(data, list):
				data = [data]
			for r in data:
				if not isinstance(r, dict):
					raise MessageError("Invalid payload")
				if "id" not in r:
					raise MessageError("Required data missing: id")
				id = r["id"]
				uid = r.get("uid", None)
				isSuccess = not ("error" in r and r["error"])
				if isSuccess:
					if "result" not in r:
						raise MessageError("Required data missing: result")
					result = r["result"]
				else:
					result = r["error"]
				self._parseResult(id, isSuccess, result, uid)
		elif self.requestProtocol == "FSP":
			if self.messageProtocol == "STR":
				payload = bytes.fromhex(payload.decode(charset))
			try:
				data = fsPacker.loads(
					payload,
					maxDictSize=0,
					maxOPSize=0,
				)
			except:
				raise MessageError("Invalid payload")
			if not isinstance(data, list):
				data = [data]
			for chunk in data:
				if isinstance(chunk, tuple) and len(chunk) == 4 and \
				(isinstance(chunk[0], (int, str)) or chunk[0] is None) and \
				isinstance(chunk[1], bool) and \
				(isinstance(chunk[3], str) or chunk[3] is None):
					self._parseResult(*chunk)
				else:
					raise MessageError("Invalid payload")
	def _parseResult(self, id:Union[int, str], isSuccess:bool, result:Any, uid:str) -> None:
		if id not in self.requests:
			self.log.warn("Got unexpected result id: {}", id)
			return
		obj = self.requests[id]
		self.log.debug("Received result for ID: {} UID: {}".format(id, uid))
		obj._parseResponse( id, isSuccess, result, uid )
	def clear(self) -> None:
		self.requests.clear()
	def clone(self, **kwargs:Dict[str, Any]) -> Client:
		opts = self.__getstate__()
		opts.update(kwargs)
		return Client(**opts)
	def close(self) -> None:
		if hasattr(self, "socket"):
			self.socket.close()
			del self.socket
			self.clear()
	def request(self, method:str, args:List[Any]=[], kwargs:Dict[str, Any]={}, id:Optional[Union[str, int]]=None,
	auth:Optional[str]=None) -> Any:
		if id is None:
			id = self.id
			self.id += 1
		if type(id) not in [int, str]:
			raise RequestError("Request ID can be only str or int")
		if id in self.requests:
			raise RequestError("Request ID already in use: {}".format(id))
		obj = Request(
			self,
			id,
			method,
			args,
			kwargs,
			auth,
		)
		self.requests[id] = obj
		self.log.info("Request queued: {} [{}]".format(method, id))
		if hasattr(self, "socket"):
			self._sendRequest( obj._dumps() )
		return obj

class Request(T_Request):
	__slots__ = ("_client", "_id", "_method", "_args", "_kwargs", "_auth", "_requestTime", "_responseTime", "_uid", "_done",
	"_success", "_response")
	def __init__(self, client:T_Client, id:Any, method:str, args:List[Any], kwargs:Dict[str, Any], auth:Optional[str]=None):
		self._client = client
		self._id = id
		self._method = method
		self._args = args
		self._kwargs = kwargs
		self._auth = auth
		self._requestTime = monotonic()
		self._responseTime = 0.0
		self._uid = ""
		self._done = False
		self._success = False
		self._response = None
	def _get(self) -> None:
		self._client._get(self._id)
		return None
	def _parseResponse(self, id:Union[int, str], isSuccess:bool, result:Any, uid:str) -> None:
		self._done = True
		self._responseTime = monotonic()
		self._uid = uid
		self._success = isSuccess
		self._response = result
	def _dumps(self) -> Any:
		if self._client.requestProtocol == "JSONRPC-2":
			return {
				"jsonrpc":"2.0",
				"params":self._kwargs or self._args,
				"method":self._method,
				"id":self._id,
			}
		elif self._client.requestProtocol == "JSONRPC-P":
			return {
				"jsonrpc":"python",
				"args":self._args,
				"kwargs":self._kwargs,
				"method":self._method,
				"id":self._id,
			}
		elif self._client.requestProtocol == "FSP":
			return (self._id, self._method, self._args, self._kwargs, self._auth)
	def get(self) -> Any:
		if not self._done:
			self._get()
		return self._response
	def getDelay(self) -> float:
		if not self._done:
			self._get()
		return self._requestTime - self._responseTime
	def getID(self) -> Any:
		return self._id
	def isDone(self) -> bool:
		return self._done
	def getUID(self) -> str:
		if not self._done:
			self._get()
		return self._uid
	def isSuccess(self) -> bool:
		if not self._done:
			self._get()
		return self._success

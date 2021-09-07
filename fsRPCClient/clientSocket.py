# Builtin modules
import re, ssl, traceback, socket, errno, codecs
from time import monotonic
from typing import Tuple, Dict, Union, Callable, NoReturn, Optional, cast
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE
from math import ceil
# Third party modules
import fsPacker
# Local modules
from . import __version__
from .utils import Headers, deflate
from .exceptions import SocketError, MessageError
from .abcs import (T_Socket, T_Client, T_BaseClientSocket, T_HTTPClientSocket, T_StringClientSocket, T_FSPackerClientSocket,
SSLContext)
# Program
NOT_CONNECTED = 0
CONNECTING    = 1
CONNECTED     = 2

class BaseClientSocket(T_BaseClientSocket):
	def __init__(self, client:T_Client, protocol:str, target:Union[str, Tuple[str, int], Tuple[str, int, int, int]],
	connectionTimeout:Union[int, float], transferTimeout:Union[int, float], ssl:bool, sslHostname:Optional[str]):
		assert protocol in ["TCPv4", "TCPv6", "IPC"], "Unsupported protocol"
		self.client            = client
		self.protocol          = protocol
		self.target            = target
		self.connectionTimeout = float(connectionTimeout)
		self.transferTimeout   = float(transferTimeout)
		self.ssl               = ssl
		self.sslHostname       = sslHostname
		self.log               = self.client.log.getChild("socket")
		self.signal            = self.client.signal
		self.poll              = DefaultSelector()
		self._reset()
		self.log.debug("Initialized")
	def _connect(self, initial:bool=False) -> bool:
		if not hasattr(self, "sock"): return True
		if initial:
			if self.protocol == "TCPv4":
				self.log.info("Connecting to {}:{} ..".format(*self.target))
			elif self.protocol == "TCPv6":
				self.log.info("Connecting to {} {} [{}/{}] ..".format(*self.target))
			elif self.protocol == "IPC":
				self.log.info("Connecting to {} ..".format(self.target))
		cerr = self.sock.connect_ex(self.target)
		if cerr in [errno.EAGAIN, errno.EINPROGRESS]:
			return False
		elif cerr in [0, errno.EISCONN]:
			if self.ssl:
				self.sslTimer = monotonic()
				sslCtx = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
				sslCtx.set_ciphers(
					":".join([
						"ECDHE-ECDSA-AES256-GCM-SHA384",
						"ECDHE-RSA-AES256-GCM-SHA384",
						"DHE-RSA-AES256-GCM-SHA384",
						"ECDHE-ECDSA-CHACHA20-POLY1305",
						"ECDHE-RSA-CHACHA20-POLY1305",
						"DHE-RSA-CHACHA20-POLY1305",
						"ECDHE-ECDSA-AES256-SHA384",
						"ECDHE-RSA-AES256-SHA384",
						"DHE-RSA-AES256-SHA256",
						"ECDHE-ECDSA-AES256-SHA",
						"ECDHE-RSA-AES256-SHA",
						"DHE-RSA-AES256-SHA",
						"RSA-PSK-AES256-GCM-SHA384",
						"DHE-PSK-AES256-GCM-SHA384",
						"RSA-PSK-CHACHA20-POLY1305",
						"DHE-PSK-CHACHA20-POLY1305",
						"ECDHE-PSK-CHACHA20-POLY1305",
						"AES256-GCM-SHA384",
						"PSK-AES256-GCM-SHA384",
						"PSK-CHACHA20-POLY1305",
						"ECDHE-PSK-AES256-CBC-SHA384",
						"ECDHE-PSK-AES256-CBC-SHA",
						"SRP-RSA-AES-256-CBC-SHA",
						"SRP-AES-256-CBC-SHA",
						"RSA-PSK-AES256-CBC-SHA384",
						"DHE-PSK-AES256-CBC-SHA384",
						"RSA-PSK-AES256-CBC-SHA",
						"DHE-PSK-AES256-CBC-SHA",
						"AES256-SHA",
						"PSK-AES256-CBC-SHA384",
						"PSK-AES256-CBC-SHA"
					])
				)
				self.sock = cast(SSLContext, sslCtx).wrap_socket(
					self.sock, False, False, server_hostname=self.sslHostname or self.target[0]
				)
				self.log.debug("Doing SSL handshake..")
			self.connectionStatus = CONNECTING
			self._setMask(EVENT_READ | EVENT_WRITE)
			if not self.ssl:
				self.connectionStatus = CONNECTED
				connectionDelay =  monotonic()-self.timeoutTimer
				self.timeoutTimer = monotonic()
				self.log.info("Connected in {:.3F} sec".format(connectionDelay))
		elif not initial:
			self._raiseSocketError("Connection failed {}[{}]".format(errno.errorcode[cerr], cerr))
		return False
	def _createSocket(self) -> None:
		if hasattr(self, "sock"): return None
		self._reset()
		self.sock = cast(T_Socket, socket.socket(
			{
				"TCPv4":socket.AF_INET,
				"TCPv6":socket.AF_INET6,
				"IPC":socket.AF_UNIX,
			}[self.protocol]
		))
		self.sock.setblocking(False)
		self.sockFD = self.sock.fileno()
		self.mask = EVENT_READ
		self.poll.register(self.sockFD, EVENT_READ)
		self.log.info("Socket created: {}".format(self.sockFD))
	def _doSSLHandshake(self) -> bool:
		try:
			self.sock.do_handshake()
		except ssl.SSLWantReadError:
			self._setMask(EVENT_READ)
			return False
		except ssl.SSLWantWriteError:
			self._setMask(EVENT_WRITE)
			return False
		except Exception:
			self._raiseSocketError("SSL Handsake error")
		self._setMask(EVENT_READ | EVENT_WRITE)
		self.log.info("Connected in {:.3F} sec [SSL: {:.3F} sec]".format(
			monotonic()-self.timeoutTimer,
			monotonic()-self.sslTimer,
		))
		self.connectionStatus = CONNECTED
		return False
	def _haveRead(self) -> bool:
		if not hasattr(self, "sock"): return True
		data = b""
		if self.ssl and self.connectionStatus == CONNECTING:
			return self._doSSLHandshake()
		try:
			data = self.sock.recv(16<<20)
		except ssl.SSLWantReadError:
			self._setMask(EVENT_READ)
			return False
		except ssl.SSLWantWriteError:
			self._setMask(EVENT_WRITE)
			return False
		except BlockingIOError:
			pass
		except ConnectionRefusedError:
			self._raiseSocketError("Connection {}".format("broken" if self.connectionStatus == CONNECTED else "refused"))
		except:
			self._raiseSocketError("Unknown error: {}".format(traceback.format_exc()))
		if not data:
			self._raiseSocketError("Connection broken")
		self.readBuffer += data
		if self.log.isFiltered("TRACE"):
			self.log.debug("Read {} bytes [{} bytes in buffer]:\n{!r}".format(
				len(data),
				len(self.readBuffer),
				data,
			))
		elif self.log.isFiltered("DEBUG"):
			self.log.debug("Read {} bytes [{} bytes in buffer]".format(
				len(data),
				len(self.readBuffer),
			))
		if self.parseReadBuffer():
			return True
		self.timeoutTimer = monotonic()
		return False
	def _haveWrite(self) -> bool:
		if not hasattr(self, "sock"): return True
		sentLength = 0
		if self.connectionStatus == NOT_CONNECTED:
			return self._connect()
		if self.connectionStatus == CONNECTING and self.ssl:
			return self._doSSLHandshake()
		if not self.writeBuffer:
			self._setMask(EVENT_READ)
			return False
		try:
			sentLength = self.sock.send(self.writeBuffer)
		except ssl.SSLWantReadError:
			self._setMask(EVENT_READ)
		except ssl.SSLWantWriteError:
			self._setMask(EVENT_WRITE)
		except BrokenPipeError:
			self._raiseSocketError("Connection broken")
		if sentLength:
			self.timeoutTimer = monotonic()
			if self.log.isFiltered("TRACE"):
				self.log.trace("Sent {} bytes [{} still in buffer]:\n{!r}".format(
					sentLength,
					len(self.writeBuffer)-sentLength,
					self.writeBuffer[:sentLength],
				))
			elif self.log.isFiltered("DEBUG"):
				self.log.debug("Sent {} bytes [{} still in buffer]".format(
					sentLength,
					len(self.writeBuffer)-sentLength,
				))
			self.writeBuffer = self.writeBuffer[sentLength:]
		if not self.writeBuffer:
			self._setMask(EVENT_READ)
		return False
	def _raiseSocketError(self, err:str) -> NoReturn:
		self.log.error(err)
		raise SocketError(err) from None
	def _raiseMessageError(self, err:str) -> NoReturn:
		self.log.error(err)
		raise MessageError(err) from None
	def _reset(self) -> None:
		self.readBuffer = b""
		self.writeBuffer = b""
		self.connectionStatus = NOT_CONNECTED
		self.timeoutTimer = 0.0
		if hasattr(self, "sock"):
			del self.sock
		self.mask = EVENT_READ
		self.sslTimer = 0.0
	def _setMask(self, newMask:int) -> None:
		if self.mask != newMask:
			self.mask = newMask
			self.log.trace("New mask: {}", newMask)
			self.poll.modify(self.sockFD, newMask)
		return None
	def _write(self, data:bytes) -> None:
		self.writeBuffer += data
		if self.connectionStatus == CONNECTED:
			self._setMask(EVENT_READ | EVENT_WRITE)
	def close(self) -> None:
		if hasattr(self, "sock"):
			self.mask = EVENT_READ
			try: self.poll.unregister(self.sockFD)
			except: pass
			try: self.sock.close()
			except: pass
			self._reset()
			self.log.info("Closed")
	def connect(self) -> None:
		if self.connectionStatus != NOT_CONNECTED:
			return
		self._createSocket()
		self.timeoutTimer = monotonic()
		self._setMask(EVENT_WRITE)
		self._connect(initial=True)
		if self.connectionStatus != CONNECTED:
			self.loop(self._isConnected)
	def _isConnected(self) -> bool:
		return self.connectionStatus == CONNECTED
	def isAlive(self) -> bool:
		return not (
			(self.connectionStatus == NOT_CONNECTED) or
			(self.connectionStatus == CONNECTING and monotonic()-self.timeoutTimer > self.connectionTimeout) or
			(self.connectionStatus == CONNECTED and monotonic()-self.timeoutTimer > self.transferTimeout)
		)
	def loop(self, whileFn:Callable[[], bool]) -> None:
		checkTimer = monotonic()
		try:
			while whileFn():
				self.signal.check()
				if monotonic()-checkTimer >= 1:
					checkTimer = monotonic()
					if not self.isAlive():
						self._raiseSocketError("Timeout")
				pollReturn = self.poll.select(1)
				for sk, pollBitmask in pollReturn:
					if sk.fd == self.sockFD:
						if pollBitmask & EVENT_READ and self._haveRead():
							return None
						if pollBitmask & EVENT_WRITE and self._haveWrite():
							return None
		except:
			self.close()
			raise
	def parseReadBuffer(self) -> bool:
		raise RuntimeError

class HTTPClientSocket(BaseClientSocket, T_HTTPClientSocket):
	defaultHeaders = {
		"User-Agent":"Fusion Solutions RPC Client v{}".format(__version__),
		"Accept":"*/*",
		"Connection":"Keep-Alive",
	}
	def __init__(self, client:T_Client, protocol:str, target:Union[str, Tuple[str, int], Tuple[str, int, int, int]],
	connectionTimeout:Union[int, float], transferTimeout:Union[int, float], ssl:bool=False, sslHostname:Optional[str]=None,
	extraHeaders:Dict[str, str]={}, disableCompression:bool=False) -> None:
		super().__init__(client, protocol, target, connectionTimeout, transferTimeout, ssl, sslHostname)
		self.headers = Headers(self.defaultHeaders)
		self.headers.update(extraHeaders)
		if not disableCompression:
			self.headers.update({"Accept-Encoding":"deflate"})
		return None
	def parseReadBuffer(self) -> bool:
		endLine = b"\r\n"
		pos = self.readBuffer.find(endLine*2)
		if pos == -1:
			endLine = b"\n"
			pos = self.readBuffer.find(endLine*2)
		elif pos > 4096:
			self._raiseMessageError("HTTP headers are too long")
		endLineLen = len(endLine)
		while pos != -1:
			rawData = self.readBuffer[pos+endLineLen*2:]
			rawHeaders = self.readBuffer[:pos].decode("ISO-8859-1").split(endLine.decode("ISO-8859-1"))
			if not rawHeaders:
				self._raiseMessageError("Invalid HTTP headers")
			httpResponse = rawHeaders.pop(0).split(" ")
			if len(httpResponse) < 2:
				self._raiseMessageError("Invalid HTTP response code")
			# if httpResponse[1] == "503":
			# 	self._raiseSocketError("Server offline")
			# elif httpResponse[1] != "200":
			# 	self._raiseSocketError("Request failure")
			headers = Headers()
			for rawHeader in rawHeaders:
				s = rawHeader.find(":")
				if s <= 0 or s > 64:
					self._raiseMessageError("Header key too long")
					return
				headers[ rawHeader[:s].strip() ] = rawHeader[s+1:].strip()
			cLengthStr = headers.get("content-length", "")
			cLength:Optional[int] = None
			if cLengthStr:
				if not cLengthStr.isdigit():
					self._raiseMessageError("Invalid HTTP header value for content-length")
				cLength = int(cLengthStr)
				if cLength < 0:
					self._raiseMessageError("Invalid HTTP header value for content-length")
			elif "chunked" not in headers.get("transfer-encoding", "").lower():
				self._raiseMessageError("Invalid HTTP transfer encoding. Not chunked and no content-length given.")
			cEncoding = headers.get("content-encoding", "")
			if cEncoding == "":
				compression = False
			elif cEncoding == "deflate":
				compression = True
			else:
				self._raiseMessageError("Request HTTP encoding not supported")
			cType = headers.get("content-type", "").split(";")[0].strip().lower()
			if cType == "":
				self._raiseMessageError("Invalid HTTP header value for content-type")
			cTypeS = re.findall("charset=([a-z0-9-]*)", headers.get("content-type", ""), re.I)
			charset = cTypeS[0] if len(cTypeS) == 1 else "iso-8859-1"
			try:
				codecs.lookup(charset)
			except LookupError:
				self._raiseMessageError("Not supported charset")
			headers["content-type"] = cType
			#
			payload = b""
			if cLength is None:
				# Chunked: data_size:int + endline:bytes + data:bytes
				rawDataCache = rawData
				EOC = False
				pos = rawDataCache.find(endLine)
				cLength = 0
				while pos != -1:
					cLength += endLineLen
					if pos == 0:
						# end of chunks
						EOC = True
						break
					rawChunk = rawDataCache[:pos]
					rawDataCache = rawDataCache[pos+endLineLen:]
					try:
						chunkLength = int(rawChunk, 16)
					except:
						self._raiseMessageError("Invalid HTTP chunk length")
					if chunkLength > 0xFFFFFF:
						self._raiseMessageError("HTTP chunk too big")
					if chunkLength > len(rawDataCache):
						return False
					payload += rawDataCache[:chunkLength]
					cLength += chunkLength
					rawDataCache = rawDataCache[chunkLength:]
					pos = rawDataCache.find(endLine)
				if not EOC:
					return False
			else:
				if len(rawData) < cLength:
					return False
				payload = rawData[:cLength]
			assert isinstance(cLength, int)
			self.readBuffer = self.readBuffer[pos+endLineLen*2+cLength:]
			#
			if compression:
				try:
					payload = deflate.decompress(payload)
				except:
					self._raiseMessageError("Invalid response content")
			if self.log.isFiltered("TRACE"):
				self.log.trace("Payload [len: {}]: {}", len(payload), payload)
			self.client._parseResponse(payload, headers, charset)
			pos = self.readBuffer.find(endLine*2)
		return False
	def send(self, payload:bytes=b"", path:str="/", headers:Dict[str, str]={}) -> None:
		rawHeader = "{} {} HTTP/1.1\r\n".format("POST" if payload else "GET", path)
		if payload:
			headers["content-length"] = str(len(payload))
		rawHeader += self.headers.dumps(extend=headers)
		rawHeader += "\r\n\r\n"
		self._write( rawHeader.encode("iso-8859-1") + payload )

class StringClientSocket(BaseClientSocket, T_StringClientSocket):
	def parseReadBuffer(self) -> bool:
		pos = self.readBuffer.find(b"\n")
		while pos != -1:
			response = self.readBuffer[:pos]
			self.readBuffer = self.readBuffer[pos+1:]
			self.client._parseResponse(response)
			pos = self.readBuffer.find(b"\n")
		return False
	def send(self, payload:bytes) -> None:
		self._write( payload + b"\n" )
		return None

class FSPackerClientSocket(BaseClientSocket, T_FSPackerClientSocket):
	def parseReadBuffer(self) -> bool:
		while not self.signal.get():
			rbl = len(self.readBuffer)
			if rbl == 0:
				return False
			indicatorlength, messageLength = fsPacker.unpackMessage(self.readBuffer)
			if indicatorlength == 0:
				self._raiseMessageError("Got invalid response")
			if rbl < indicatorlength:
				return False
			payloadLength = indicatorlength+messageLength
			if rbl < payloadLength:
				return False
			response = self.readBuffer[indicatorlength:payloadLength]
			self.readBuffer = self.readBuffer[payloadLength:]
			self.client._parseResponse(response)
		return False
	def send(self, payload:bytes) -> None:
		self._write( fsPacker.packMessage( payload ) )
		return None

class OldFSProtocolClientSocket(BaseClientSocket, T_FSPackerClientSocket):
	def _readResponse(self) -> Tuple[int, bytes]:
		fl = len(self.readBuffer)
		if not fl:
			return 0, b""
		li = self.readBuffer[0]
		if li == 0:
			return -1, b""
		if fl < li:
			return 0, b""
		l = int.from_bytes(self.readBuffer[ 1:1+li ], "little")
		tl = 1+li+l
		if fl < tl:
			return 0, b""
		return tl, self.readBuffer[ 1+li:tl ]
	def _encodeRequest(self, buffer:bytes) -> bytes:
		l = len(buffer)
		li = ceil(l.bit_length() / 8)
		return li.to_bytes(1, "little") + l.to_bytes(li, "little") + buffer
	def parseReadBuffer(self) -> bool:
		while not self.signal.get():
			if not self.readBuffer:
				return False
			messageLength, response = self._readResponse()
			if messageLength == -1:
				return True
			if messageLength == 0:
				return False
			self.readBuffer = self.readBuffer[messageLength:]
			self.client._parseResponse(response)
		return False
	def send(self, payload:bytes) -> None:
		self._write( self._encodeRequest( payload ) )
		return None

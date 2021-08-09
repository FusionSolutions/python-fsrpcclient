# Builtin modules
from __future__ import annotations
from abc import ABCMeta, abstractmethod
from socket import socket
from selectors import BaseSelector
from typing import List, Dict, Tuple, Union, Callable, Optional, Any, MutableMapping
# Third party modules
from fsLogger import Logger
from fsSignal import T_Signal
# Local modules
from .utils import Headers
# Program
class T_Socket(socket, metaclass=ABCMeta):
	@abstractmethod
	def do_handshake(self) -> None: ...

class T_Client(metaclass=ABCMeta):
	max_bulk_request:int
	target:Union[str, Tuple[str, int], Tuple[str, int, int, int]]
	protocol:str
	connTimeout:int
	transferTimeout:int
	retryCount:int
	retryDelay:int
	ssl:bool
	sslHostname:Optional[str]
	httpHost:Optional[str]
	extraHttpHeaders:Dict[str, str]
	httpMethod:str
	httpPath:str
	disableCompression:bool
	log:Logger
	signal:T_Signal
	id:int
	requests:MutableMapping[Optional[Union[str, int]], T_Request]
	socket:Union[T_HTTPClientSocket, T_StringClientSocket, T_FSPackerClientSocket]
	socketProtocol:str
	messageProtocol:str
	requestProtocol:str
	@abstractmethod
	def _parseResponse(self, payload:bytes, headers:Optional[Headers]=..., charset:str=...) -> None: ...
	@abstractmethod
	def _get(self, id:Any) -> None: ...

class T_BaseClientSocket(metaclass=ABCMeta):
	#
	client:T_Client
	protocol:str
	target:Union[str, Tuple[str, int], Tuple[str, int, int, int]]
	connTimeout:float
	transferTimeout:float
	ssl:bool
	sslHostname:Optional[str]
	log:Logger
	signal:T_Signal
	poll:BaseSelector
	#
	readBuffer:bytes
	writeBuffer:bytes
	connectionStatus:int
	timeoutTimer:float
	sock:T_Socket
	sockFD:int
	mask:int
	sslTimer:float
	@abstractmethod
	def isAlive(self) -> bool: ...
	@abstractmethod
	def connect(self) -> None: ...
	@abstractmethod
	def close(self) -> None: ...
	@abstractmethod
	def loop(self, whileFn:Callable[[], bool]) -> None: ...

class T_HTTPClientSocket(T_BaseClientSocket, metaclass=ABCMeta):
	defaultHeaders:Dict[str, str]
	headers:Headers
	httpMethod:str
	path:str
	@abstractmethod
	def send(self, payload:bytes, headers:Dict[str, str]=...) -> None: ...

class T_StringClientSocket(T_BaseClientSocket, metaclass=ABCMeta):
	@abstractmethod
	def send(self, payload:bytes) -> None: ...

class T_FSPackerClientSocket(T_BaseClientSocket, metaclass=ABCMeta):
	@abstractmethod
	def send(self, payload:bytes) -> None: ...

class T_Request(metaclass=ABCMeta):
	_client:T_Client
	_id:Optional[Union[str, int]]
	_method:str
	_args:List[Any]
	_kwargs:Dict[Any, Any]
	_requestTime:float
	_responseTime:float
	_uid:str
	_done:bool
	_success:bool
	_response:Any
	@abstractmethod
	def _parseResponse(self, id:Union[int, str], isSuccess:bool, result:Any, uid:str) -> None: ...
	@abstractmethod
	def _dumps(self) -> Any: ...
	@abstractmethod
	def get(self) -> Any: ...
	@abstractmethod
	def getDelay(self) -> float: ...
	@abstractmethod
	def getID(self) -> Any: ...
	@abstractmethod
	def isDone(self) -> bool: ...
	@abstractmethod
	def getUID(self) -> str: ...
	@abstractmethod
	def isSuccess(self) -> bool: ...

# Builtin modules
import unittest
from typing import Type
# Third party modules
from fsLogger import SimpleLogger, Logger
from fsSignal import Signal, BaseSignal
# Local modules
from .. import Client
# Program
class ClientTest(unittest.TestCase):
	signal:Type[BaseSignal]
	@classmethod
	def setUpClass(cls) -> None:
		SimpleLogger("TRACE")
		cls.signal = Signal.getSoftSignal()
		return None
	def test_first(self) -> None:
		rootClient = Client(
			"TCPv4:HTTP:JSONRPC-2",
			("api.fusionexplorer.io", 443),
			ssl=True,
			disableCompression=True,
			log=Logger("Client"),
			signal=self.signal
		)
		r = rootClient.request("ping")
		self.assertEqual(r.isDone(), False)
		self.assertEqual(r.isSuccess(), True)
		self.assertEqual(r.isDone(), True)
		self.assertGreater(r.getDelay(), 0)
		self.assertRegex(r.get(), r"0x[0-9a-zA-Z]")
		with rootClient as c:
			r = c.request("ping")
			self.assertEqual(r.isDone(), False)
			self.assertEqual(r.isSuccess(), True)
			self.assertEqual(r.isDone(), True)
			self.assertRegex(r.get(), r"0x[0-9a-zA-Z]")
			r = c.request("ping", id=88)
			self.assertEqual(r.getID(), 88)
			r = c.request("ping", [0])
			self.assertEqual(r.isSuccess(), False)
			self.assertEqual(r.isDone(), True)
			self.assertEqual(r.get(), {
				"code": -32602,
				"data": "ping() takes 0 positional argument but 1 were given",
				"message": "Invalid params"
			})
			r = c.request("surenoteexists")
			self.assertEqual(r.isSuccess(), False)
			self.assertEqual(r.isDone(), True)
			self.assertEqual(r.get(), {"code": -32601, "message": "Method not found"})
			r0 = c.request("ping", id="asd")
			r1 = c.request("ping", id="dsa")
			self.assertRegex(r0.get(), r"0x[0-9a-zA-Z]")
			self.assertRegex(r1.get(), r"0x[0-9a-zA-Z]")
		with rootClient as c:
			r0 = c.request("ping", id="asd")
			r1 = c.request("ping", id="dsa")
			self.assertRegex(r0.get(), r"0x[0-9a-zA-Z]")
			self.assertRegex(r1.get(), r"0x[0-9a-zA-Z]")

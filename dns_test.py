from dnslib.server import DNSServer, DNSHandler, BaseResolver
from dnslib import RR, QTYPE, A

REQUEST_MAP = {
    "request-abcd1234.bobs.com.": "10.0.0.101",
    "request-efgh1234.bobs.com.": "10.0.0.102",
}

class SimpleResolver(BaseResolver):
    def resolve(self, request, handler):
        qname = str(request.q.qname)
        qtype = QTYPE[request.q.qtype]
        reply = request.reply()

        if qtype == "A" and qname in REQUEST_MAP:
            ip = REQUEST_MAP[qname]
            reply.add_answer(RR(qname, QTYPE.A, rdata=A(ip), ttl=30))

        return reply

resolver = SimpleResolver()
server = DNSServer(resolver, port=8553, address="0.0.0.0", tcp=True)
server.start_thread()

import time
while True:
    time.sleep(1)

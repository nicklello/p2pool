
TestNet = False
Address = "1MjeEv3WDgycrEaaNeSESrWvRfkU6s81TX"
workerEndpoint = "3333"
DonationPercentage = 0.0
Upnp = True
BitcoindConfigPath = "/opt/bitcoin/bitcoindata/bitcoin.conf"

WORKER_STATUS_REFRESH_TIME = 10

dbService = {}
workerStatus = {}

NodeService = {
    'authentication': 'http://127.0.0.1:8080/service/node/authentication.htm'
}

DbOptions = {
    'type': 'sql',
    'engine': 'mysql',
    'dbopts': {
        'host': '127.0.0.1',
        'db': 'antpooldb',
        'user': 'antpool',
        'password': 'antpool',
    }
}

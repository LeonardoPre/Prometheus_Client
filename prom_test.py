import datetime
from prometheus_api_client import PrometheusConnect
import ipaddress



class PrometheusCollector:
    def __init__(self, prometheus_url: str):
        if not prometheus_url.startswith("http"):
            prometheus_url = f"http://{prometheus_url}"
            
        self.prm = PrometheusConnect(url=prometheus_url, disable_ssl=True)
        if not self.prm.check_prometheus_connection():
            raise ValueError(f"Could not connect to Prometheus at {prometheus_url}")
    
    @staticmethod
    def is_valid_ip(instance_string):
        ip_part, sep, port = instance_string.rpartition(':')
        
        address_to_check = ip_part if sep else port
        try:
            ipaddress.ip_address(address_to_check)
            return True
        except ValueError:
            return False
    
    @property
    def node_map(self):
        query = f"kube_node_info{{app_kubernetes_io_instance='prometheus'}}"
        nodes = self.prm.custom_query(query)
        node_map = {}
        for node in nodes:
            ip = node["metric"]["internal_ip"]
            node_name = node["metric"]["node"]
            node_map[ip] = node_name
        
        return node_map

    def fetch(self): 
        sumby = "node"

        shelly = "shelly_apower_watts"
        kepler = f"sum by ({sumby}) (irate(kepler_node_package_joules_total[60s])) + sum by ({sumby}) (irate(kepler_node_dram_joules_total[60s]))"
        kepler_new = f"sum by ({sumby}) (irate(kepler_node_cpu_joules_total{{zone=~'dram|package'}}[60s]))"
        scaphandre = f"sum by ({sumby}) (scaph_host_power_microwatts/1e6)"

        
        query_strings = [shelly, kepler, kepler_new, scaphandre]
        
        results = {}
        names = ["shelly", "kepler_old", "kepler_new", "scaphandre"]
        
        for name, q in zip(names, query_strings):
            results[name] = self.get_node_metrics(self.prm.custom_query(q))
            
        return results
    
    def get_node_metrics(self, _results):
        results = {}
        for node in _results:
            metric = node['metric']
            if 'node' in metric:
                name = metric['node']
            elif 'instance' in metric:
                if self.is_valid_ip(metric['instance']):
                    ip = metric['instance'].split(":")[0]
                    name = self.node_map[ip]
                else:
                    name = metric['instance']
            else:
                print("metric has neither instance nor node info: %s", node)
                continue
            results[name] = {
                "timestamp": datetime.datetime.fromtimestamp(node['value'][0]),
                "value": node['value'][1]
            }
        return results
    


if __name__ == "__main__":
    collector = PrometheusCollector("http://localhost:9090")
    print(collector.node_map)
    nodes_data = collector.fetch()
    for name,node_data in nodes_data.items():
        print(f"{name}:")
        print(node_data)
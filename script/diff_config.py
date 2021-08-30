import os
import subprocess
import io
import xlwt
import xml.etree.ElementTree as ET

CONF_ROOT_DIR = "/software/conf"
OUTPUT_DIFF_FILE_NAME = "./hive_client_conf_diff.xls"
OUTPUT_META_DIFF_FILE_NAME = "./hive_metastore_conf_diff.xls"
OUTPUT_PROP_FILE_NAME = "./hive_client_conf.xls"
COMPARE_CONF_FILE_NAME = "hive_conf/hive-site.xml"
METASTORE_URIS = "hive.metastore.uris"
CLUSTERS = ["test1"]
IGNORE_CONFIG_PROPS = [
    "hive.metastore.uris",
    "hive.metastore.warehouse.dir",
    "hive.exec.scratchdir",
    "hive.exec.stagingdir"
]
IGNORE_METASTORE_CONFIG_PROPS = [
    "hive.metastore.warehouse.dir",
    "hive.client.cluster.id",
    "hive.mart.db.relation.location.url",
    "javax.jdo.option.ConnectionPassword",
    "javax.jdo.option.ConnectionURL",
    "hive.urm.api.ddl.create",
    "hive.urm.api.show.database",
    "hive.urm.api.show.table",
    "hive.urm.api.check.right",
    "hive.urm.api.ddl.alter",
    "hive.hwi.listen.host",
    "hive.policy.increment.url",
    "hive.delete.table.policy.url",
    "hive.policy.all.url",
    "hive.urm.api.ddl.drop",
    "hive.authenticator.urm.timeout",
    "hive.jd.check.slave.db.delay.token",
    "hive.jd.use.slave.db.enable",
    "hive.jd.salve.db.javax.jdo.option.ConnectionURL",
    "hive.jd.salve.db.javax.jdo.option.ConnectionPassword",
    "hive.jd.salve.db.javax.jdo.option.ConnectionUserName"
]
cmp_template_prop = None
cmp_metasore_template_prop = None
complete_metastore = ['127.0.0.1']
complete_compare_metastore = ['127.0.0.1']
scp_failed_ips = []


class XmlParse:

    def __init__(self, xml_file):
        self.xml_file = xml_file
        self.properties = {}

    def parse(self, ignore_config_props=None):
        if ignore_config_props is None:
            ignore_config_props = []
        tree = ET.parse(self.xml_file)
        root = tree.getroot()

        for prop in root.findall('property'):
            prop_name = prop.find("name").text
            if prop_name not in ignore_config_props:
                self.properties[prop_name] = prop.find("value").text
        return self.properties


class XmlWrite:

    def __init__(self, xml_file):
        self.xml_file = xml_file
        self.workbook = xlwt.Workbook()

    def write_conf_diff(self, cluster_name, market_conf_diffs):
        table = self.workbook.add_sheet(cluster_name)
        table.write(0, 0, u'集市')
        table.write(0, 1, u'队列')
        table.write(0, 2, u'参数')
        table.write(0, 3, u'当前值')
        table.write(0, 4, u'比对值')
        table.write(0, 5, u'类型')
        count = 0
        for market, queues in market_conf_diffs.items():
            if len(queues) == 0:
                continue
            table.write(count + 1, 0, os.path.basename(market))
            count = count + 1
            for queue, configs in queues.items():
                if len(configs) == 0:
                    continue
                table.write(count + 1, 1, os.path.basename(queue))
                for k, v in configs.items():
                    table.write(count + 1, 2, k)
                    table.write(count + 1, 3, v["value"])
                    table.write(count + 1, 4, v["cmp_value"])
                    table.write(count + 1, 5, v["diff_type"])
                    count = count + 1

    def write_metatore_conf_diff(self, cluster_name, market_conf_diffs):
        table = self.workbook.add_sheet(cluster_name)
        table.write(0, 0, u'集市')
        table.write(0, 1, u'队列')
        table.write(0, 2, 'MetastoreUris')
        table.write(0, 3, 'Metastore')
        table.write(0, 4, u'参数')
        table.write(0, 5, u'当前值')
        table.write(0, 6, u'比对值')
        table.write(0, 7, u'类型')
        count = 0
        for market, queues in market_conf_diffs.items():
            if len(queues) == 0:
                continue
            table.write(count + 1, 0, os.path.basename(market))
            count = count + 1
            for queue, configs in queues.items():
                if len(configs['metastore_conf']) == 0:
                    continue
                table.write(count + 1, 1, os.path.basename(queue))
                table.write(count + 1, 2, configs['metastoreUris'])
                for uri, confs in configs['metastore_conf'].items():
                    print("---------%d-----%s" % ((count + 1), uri))
                    table.write(count + 1, 3, uri)
                    for k, v in confs.items():
                        table.write(count + 1, 4, k)
                        table.write(count + 1, 5, v["value"])
                        table.write(count + 1, 6, v["cmp_value"])
                        table.write(count + 1, 7, v["diff_type"])
                        count = count + 1

    def write_conf_prop(self, cluster_name, market_conf):
        table = self.workbook.add_sheet(cluster_name)
        table.write(0, 0, u'集群')
        table.write(0, 1, u'集市')
        table.write(0, 2, u'队列')
        table.write(0, 3, u'thrift地址')
        table.write(1, 0, cluster_name)
        count = 0
        for market, queues in market_conf.items():
            if len(queues) == 0:
                continue
            table.write(count + 1, 1, os.path.basename(market))
            count = count + 1
            for queue, config in queues.items():
                if len(config) == 0:
                    continue
                table.write(count + 1, 2, os.path.basename(queue))
                table.write(count + 1, 3, str(config))
                count = count + 1

    def save(self):
        self.workbook.save(self.xml_file)


def walk_dir(root_dir):
    dirs = []
    for child_dir in os.listdir(root_dir):
        path = os.path.join(root_dir, child_dir)
        if os.path.isdir(path):
            dirs.append(path)
    return dirs


def compare_dict(x, y):
    results = {}
    for k, v in x.items():
        if k in y and v != y[k]:
            results[k] = {"value": v, "cmp_value": y[k], "diff_type": u"修改"}
        elif k not in y:
            results[k] = {"value": v, "cmp_value": "", "diff_type": u"添加"}

    delete_props = {k: {"value": "", "cmp_value": y[k], "diff_type": u"缺少"} for k in y if k not in x}
    results.update(delete_props)
    return results


def scp_remote_file(ip):
    CMD = 'sshpass -p "passwd" scp -o StrictHostKeyChecking=no logview@{ip}:/software/conf/bdpops/hive_conf/hive-site.xml ./metastore/hive-site_{ip}' \
        .format(ip=ip)

    proc = subprocess.Popen(CMD, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=-1)
    proc.wait()

    stream_stdout = io.TextIOWrapper(proc.stdout, encoding='utf-8')
    stream_stderr = io.TextIOWrapper(proc.stderr, encoding='utf-8')

    str_stdout = str(stream_stdout.read())
    str_stderr = str(stream_stderr.read())

    print("stdout: " + str_stdout)
    print("stderr: " + str_stderr)
    print("returncode: " + str(proc.returncode))
    return proc.returncode


class ClusterConf:

    def __init__(self, name):
        self.name = name
        self.markets = {}
        self.market_conf = {}
        self.market_conf_diffs = {}
        self.market_metastore_conf_diffs = {}
        self.config_file = COMPARE_CONF_FILE_NAME

    def walk_market(self):
        cluster_dir = os.path.join(CONF_ROOT_DIR, self.name)
        if os.path.isdir(cluster_dir):
            for market_dir in walk_dir(cluster_dir):
                self.markets[market_dir] = walk_dir(market_dir)

    def scratch_prop(self, prop):
        for market, queues in self.markets.items():
            queue_props = {}
            for queue in queues:
                conf_file = os.path.join(queue, self.config_file)
                if not os.path.isfile(conf_file):
                    continue
                props = XmlParse(conf_file).parse()
                queue_props[queue] = props[prop]
            self.market_conf[market] = queue_props

    def scratch_metastore_configs(self):
        global scp_failed_ips
        global complete_metastore
        for market, queues in self.markets.items():

            for queue in queues:
                conf_file = os.path.join(queue, self.config_file)
                if not os.path.isfile(conf_file):
                    continue
                props = XmlParse(conf_file).parse()
                for uri in [uri[9:-5] for uri in props[METASTORE_URIS].split(",")]:
                    if uri in complete_metastore:
                        continue
                    if scp_remote_file(uri) != 0:
                        scp_failed_ips.append(uri)
                    else:
                        print("scp ip %s success." % uri)
                    complete_metastore.append(uri)

    def compare_metastore_configs(self, ignore_config_props=None):
        # cluster --> market --> queue --> metastore
        global cmp_metasore_template_prop
        global complete_compare_metastore
        for market, queues in self.markets.items():
            queue_conf_diff = {}
            for queue in queues:
                conf_file = os.path.join(queue, self.config_file)
                if not os.path.isfile(conf_file):
                    continue
                props = XmlParse(conf_file).parse()
                metatore_uris = props[METASTORE_URIS]
                metastore_conf_diff = {}
                for uri in [uri[9:-5] for uri in metatore_uris.split(",")]:
                    if uri in complete_compare_metastore:
                        continue
                    metatore_file = "./metastore/hive-site_%s" % uri
                    if cmp_metasore_template_prop is None:
                        cmp_metasore_template_prop = XmlParse(metatore_file).parse(ignore_config_props)
                        print("load template prop: " + metatore_file)
                    else:
                        cmp_props = XmlParse(metatore_file).parse(ignore_config_props)
                        metastore_conf_diff[uri] = compare_dict(cmp_props, cmp_metasore_template_prop)
                        print(metastore_conf_diff[uri])
                    complete_compare_metastore.append(uri)
                queue_conf_diff[queue] = {"metastore_conf": metastore_conf_diff, "metastoreUris": metatore_uris}
            self.market_metastore_conf_diffs[market] = queue_conf_diff

    def compare_configs(self, ignore_config_props=None):
        global cmp_template_prop
        if ignore_config_props is None:
            ignore_config_props = []
        for market, queues in self.markets.items():
            queue_conf_diffs = {}
            for queue in queues:
                conf_file = os.path.join(queue, self.config_file)
                if not os.path.isfile(conf_file):
                    continue
                if cmp_template_prop is None:
                    cmp_template_prop = XmlParse(conf_file).parse(ignore_config_props)
                    print("load template prop: " + conf_file)
                else:
                    cmp_props = XmlParse(conf_file).parse(ignore_config_props)
                    queue_conf_diffs[queue] = compare_dict(cmp_props, cmp_template_prop)
                    print(queue_conf_diffs[queue])

            self.market_conf_diffs[market] = queue_conf_diffs


def generate_conf_diff_file():
    xml_write = XmlWrite(OUTPUT_DIFF_FILE_NAME)
    for cluster in CLUSTERS:
        cluster_conf = ClusterConf(cluster)
        cluster_conf.walk_market()
        cluster_conf.compare_configs(IGNORE_CONFIG_PROPS)
        xml_write.write_conf_diff(cluster_conf.name, cluster_conf.market_conf_diffs)
    xml_write.save()


def generate_conf_file():
    xml_write = XmlWrite(OUTPUT_PROP_FILE_NAME)
    for cluster in CLUSTERS:
        cluster_conf = ClusterConf(cluster)
        cluster_conf.walk_market()
        cluster_conf.scratch_prop(METASTORE_URIS)
        xml_write.write_conf_prop(cluster_conf.name, cluster_conf.market_conf)
    xml_write.save()


def scratch_metastore_conf():
    for cluster in CLUSTERS:
        cluster_conf = ClusterConf(cluster)
        cluster_conf.walk_market()
        cluster_conf.scratch_metastore_configs()
    print(scp_failed_ips)


def generate_meta_conf_diff_file():
    xml_write = XmlWrite(OUTPUT_META_DIFF_FILE_NAME)
    for cluster in CLUSTERS:
        cluster_conf = ClusterConf(cluster)
        cluster_conf.walk_market()
        cluster_conf.compare_metastore_configs(IGNORE_METASTORE_CONFIG_PROPS)
        xml_write.write_metatore_conf_diff(cluster_conf.name, cluster_conf.market_metastore_conf_diffs)
    xml_write.save()


if __name__ == "__main__":
    generate_conf_diff_file()
    generate_conf_file()
    scratch_metastore_conf()
    generate_meta_conf_diff_file()

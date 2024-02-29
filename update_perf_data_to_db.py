import urllib

import psycopg
import requests
from bs4 import BeautifulSoup


class UpdatePerfData:
    def __init__(
        self,
        url,
        api_key,
        db_name,
        db_user,
        db_password,
        db_host,
        db_port,
        ceph_version,
        build,
        run_type,
        suite,
        component,
    ):
        self.api_key = api_key
        self.url = url
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_conn = None
        self.ceph_version = ceph_version
        self.build = build
        self.run_type = run_type
        self.suite = suite
        self.component = component

        self._initialise_db_conn()

    def fetch_perf_data(self):
        """
        Master method which fetches perf data from the given path
        """

        tests = self._get_files(self.url, ext="/")
        for test in tests:
            print(f"Test : {test}")
            data_url = f"{self.url}/{test}"
            print(data_url)
            perf_data = self._get_files(url=data_url, ext=".csv")
            if "configure_client" not in test:
                for item in perf_data:
                    print(f"\t\t{item}")
                    node = item.split("-")[-2]
                    with urllib.request.urlopen(f"{data_url}{item}") as fin:
                        # Save locally if desired.
                        text = fin.read().decode("utf-8").split("\n")[2:]
                        for entry in text:
                            if entry:
                                entry = entry.split(",")
                                timestamp = entry[0].strip("][")
                                process_name = entry[1]
                                process_type = process_name.split(".")[0]
                                cpu_usage = entry[3]
                                mem_usage = entry[4]
                                test_name = test.replace("/", "")[:-2]
                                self._insert_into_db(
                                    test_name.strip(),
                                    timestamp.strip(),
                                    process_name.strip(),
                                    node.strip(),
                                    process_type.strip(),
                                    cpu_usage.strip(),
                                    mem_usage.strip(),
                                )
        self.db_conn.close()

    def _get_files(self, url, ext=""):
        page = requests.get(url).text
        soup = BeautifulSoup(page, "html.parser")
        hrefs = []

        for a in soup.find_all("a"):
            if a["href"].endswith(ext) and not a["href"].startswith(ext):
                hrefs.append(a["href"])

        return hrefs

    def _insert_into_db(
        self,
        test_name,
        timestamp,
        process_name,
        node,
        process_type,
        cpu_usage,
        mem_usage,
    ):
        cmd = f"""INSERT INTO perf_data (ceph_version, build, component, run_type, suite, testcase, time_stamp, process, node, process_name, cpu_usage, mem_usage) VALUES ('{self.ceph_version}', '{self.build}', '{self.component}', '{self.run_type}', '{self.suite}', '{test_name}', to_timestamp('{timestamp}', 'YYYY/DD/MM HH:MI:SS'), '{process_name}', '{node}', '{process_type}', '{cpu_usage}', '{mem_usage}')"""
        self.db_conn.cursor().execute(cmd)
        print("Data entered")

    def _initialise_db_conn(self):
        self.db_conn = psycopg.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
        )
        self.db_conn.autocommit = True


obj = UpdatePerfData(
    url="http://<url>>performance-metrics/",
    api_key="<api_key>",
    db_name="postgres",
    db_user="<user>",
    db_password="<pwd>",
    db_host="<ip>",
    db_port="5432",
    ceph_version="<ceph-version>",
    build="<build-version>",
    run_type="sanity",
    suite="<suite-name>",
    component="<component>",
)
obj.fetch_perf_data()

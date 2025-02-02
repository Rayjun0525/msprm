#!/usr/bin/env python3
"""
간단한 PostgreSQL replication 관리 도구 (Patroni/pg_repmgr 모방)

설정은 YAML 파일 혹은 CLI 옵션을 통해 지정합니다.
예시 YAML 설정 파일 (config.yaml):

---
check_interval: 5               # 상태 체크 주기 (초)
connection_timeout: 2           # pg_isready 체크 시 타임아웃 (초)
promotion_cooldown: 30          # leader 프로모션 후 재프로모션까지 최소 대기 시간 (초)
failback_enabled: true          # failback 수행 여부
promotion_command: "psql -h {host} -p {port} -c \"SELECT pg_promote();\""
replication_command: "echo 'Reconfiguring replication for {name} to follow leader {leader_name} at {leader_host}:{leader_port}'"
nodes:
  - name: node1
    host: 127.0.0.1
    port: 5432
    failover_order: 1
    role: leader
    dbname: postgres
    user: postgres
    password: password123
  - name: node2
    host: 127.0.0.2
    port: 5432
    failover_order: 2
    role: replica
    dbname: postgres
    user: postgres
    password: password123
  - name: node3
    host: 127.0.0.3
    port: 5432
    failover_order: 3
    role: replica
    dbname: postgres
    user: postgres
    password: password123
---

실행 예:
    ./pg_manager.py --config config.yaml --log-level DEBUG
"""

import argparse
import logging
import subprocess
import time
import sys
import os

try:
    import yaml
except ImportError:
    print("PyYAML 모듈이 필요합니다. pip install pyyaml")
    sys.exit(1)

# psycopg2 또는 psycopg (psycopg3) 임포트
try:
    import psycopg2
    use_psycopg2 = True
except ImportError:
    try:
        import psycopg  # psycopg3
        use_psycopg2 = False
    except ImportError:
        print("psycopg2 또는 psycopg 모듈이 필요합니다. pip install psycopg2 (또는 pip install psycopg)")
        sys.exit(1)


def load_config(config_path=None):
    """
    YAML 파일에서 설정을 읽어옵니다.
    config_path가 None이면 기본 설정을 사용합니다.
    """
    default_config = {
        "check_interval": 5,
        "connection_timeout": 2,
        "promotion_cooldown": 30,
        "failback_enabled": True,
        "promotion_command": "psql -h {host} -p {port} -c \"SELECT pg_promote();\"",
        "replication_command": "echo 'Reconfiguring replication for {name} to follow leader {leader_name} at {leader_host}:{leader_port}'",
        "nodes": [
            {"name": "node1", "host": "127.0.0.1", "port": 5432, "failover_order": 1, "role": "leader", "dbname": "postgres"},
            {"name": "node2", "host": "127.0.0.2", "port": 5432, "failover_order": 2, "role": "replica", "dbname": "postgres"},
            {"name": "node3", "host": "127.0.0.3", "port": 5432, "failover_order": 3, "role": "replica", "dbname": "postgres"},
        ]
    }
    if config_path is None:
        logging.info("설정 파일 경로가 제공되지 않아 기본 설정을 사용합니다.")
        return default_config

    if not os.path.exists(config_path):
        logging.error("설정 파일 %s 이(가) 존재하지 않습니다. 기본 설정을 사용합니다.", config_path)
        return default_config

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        logging.info("설정 파일 %s 로드 완료", config_path)
        return config
    except Exception as e:
        logging.error("설정 파일 로드 중 오류 발생: %s. 기본 설정을 사용합니다.", e)
        return default_config


def health_check_with_psycopg(node, timeout):
    """
    psycopg2(또는 psycopg3)를 사용하여 해당 노드에 접속한 후 SELECT 1 쿼리를 실행합니다.
    노드 설정에 dbname, user, password가 있을 경우 사용하며, 없으면 기본값을 적용합니다.
    """
    dsn_params = {
        "host": node.get("host"),
        "port": node.get("port"),
        "connect_timeout": timeout,
        "dbname": node.get("dbname", "postgres")
    }
    if "user" in node:
        dsn_params["user"] = node["user"]
    if "password" in node:
        dsn_params["password"] = node["password"]

    try:
        if use_psycopg2:
            conn = psycopg2.connect(**dsn_params)
        else:
            conn = psycopg.connect(**dsn_params)
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        cur.close()
        conn.close()
        if result and result[0] == 1:
            return True
        else:
            logging.debug("SELECT 1 check failed for node %s", node.get("name"))
            return False
    except Exception as e:
        logging.debug("psycopg health check failed for node %s: %s", node.get("name"), str(e))
        return False


def check_node_health(node, timeout):
    """
    해당 노드에 대해 pg_isready 명령어를 사용하여 health check를 수행하고,
    psycopg를 통해 SELECT 1 쿼리 결과까지 확인합니다.
    """
    host = node.get("host")
    port = node.get("port")
")
    # pg_isready를 사용 (pg_isready가 PATH에 있어야 함)
    cmd = f"pg_isready -h {host} -p {port} -t {timeout}"
    try:
        result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            logging.debug("pg_isready check failed for node %s: %s", node.get("name"), result.stdout.decode().strip())
            return False
    except Exception as e:
        logging.debug("pg_isready execution error for node %s: %s", node.get("name"), str(e))
        return False

    # 추가로 psycopg를 통해 SELECT 1 실행 확인
    return health_check_with_psycopg(node, timeout)


def promote_node(node, promotion_command):
    """
    지정된 노드에 대해 promotion_command를 실행합니다.
    promotion_command는 포맷팅 문자열로, {name}, {host}, {port} 등의 변수를 포함할 수 있습니다.
    """
    cmd = promotion_command.format(name=node.get("name"),
                                   host=node.get("host"),
                                   port=node.get("port"))
    logging.info("노드 %s에 대해 프로모션 명령 실행: %s", node.get("name"), cmd)
    try:
        result = subprocess.run(cmd, shell=True, check=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info("프로모션 명령 성공: %s", result.stdout.decode().strip())
        return True
    except subprocess.CalledProcessError as e:
        logging.error("프로모션 명령 실패 (노드 %s): %s", node.get("name"), e.stderr.decode().strip())
        return False


def reconfigure_replication(new_leader, node, replication_command):
    """
    복제 재구성 명령을 실행합니다.
    replication_command는 포맷팅 문자열로, {name}, {leader_name}, {leader_host}, {leader_port} 등을 포함할 수 있습니다.
    """
    cmd = replication_command.format(
        name=node.get("name"),
        host=node.get("host"),
        port=node.get("port"),
        leader_name=new_leader.get("name"),
        leader_host=new_leader.get("host"),
        leader_port=new_leader.get("port")
    )
    logging.info("노드 %s 복제 재구성 명령 실행: %s", node.get("name"), cmd)
    try:
        result = subprocess.run(cmd, shell=True, check=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info("복제 재구성 명령 성공 (노드 %s): %s", node.get("name"), result.stdout.decode().strip())
    except subprocess.CalledProcessError as e:
        logging.error("복제 재구성 명령 실패 (노드 %s): %s", node.get("name"), e.stderr.decode().strip())


def update_roles(nodes, new_leader_name):
    """
    새 leader를 기준으로 각 노드의 역할을 업데이트합니다.
    """
    for node in nodes:
        if node.get("name") == new_leader_name:
            node["role"] = "leader"
        else:
            node["role"] = "replica"


def get_current_leader(nodes):
    """
    현재 역할이 leader로 지정된 노드를 반환합니다.
    만약 여러 leader가 있다면 failover_order가 가장 낮은 노드를 반환합니다.
    """
    leaders = [node for node in nodes if node.get("role") == "leader"]
    if not leaders:
        return None
    # failover_order가 낮은 순서대로 정렬
    leaders.sort(key=lambda n: n.get("failover_order", 9999))
    return leaders[0]


def get_best_candidate(nodes):
    """
    건강한 노드 중 failover_order가 가장 낮은 노드를 반환합니다.
    """
    healthy_nodes = [node for node in nodes if node.get("healthy")]
    if not healthy_nodes:
        return None
    healthy_nodes.sort(key=lambda n: n.get("failover_order", 9999))
    return healthy_nodes[0]


def main():
    parser = argparse.ArgumentParser(description="간단한 PostgreSQL replication 관리 도구")
    parser.add_argument("-c", "--config", help="YAML 설정 파일 경로", default=None)
    parser.add_argument("-l", "--log-level", help="로그 레벨 (DEBUG, INFO, WARNING, ERROR)", default="INFO")
    args = parser.parse_args()

    # 로그 레벨 설정
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        print("잘못된 로그 레벨: %s" % args.log_level)
        sys.exit(1)
    logging.basicConfig(level=numeric_level, format="%(asctime)s [%(levelname)s] %(message)s")

    config = load_config(args.config)

    check_interval = config.get("check_interval", 5)
    connection_timeout = config.get("connection_timeout", 2)
    promotion_cooldown = config.get("promotion_cooldown", 30)
    failback_enabled = config.get("failback_enabled", True)
    promotion_command = config.get("promotion_command")
    replication_command = config.get("replication_command")
    nodes = config.get("nodes", [])

    if not nodes:
        logging.error("노드 설정이 없습니다. 종료합니다.")
        sys.exit(1)

    last_promotion_time = 0

    logging.info("서비스 시작. 주기적으로 노드 상태를 체크합니다.")
    while True:
        current_time = time.time()

        # 각 노드의 건강 상태 업데이트
        for node in nodes:
            healthy = check_node_health(node, connection_timeout)
            node["healthy"] = healthy
            status = "건강" if healthy else "비정상"
            logging.debug("노드 %s (%s:%s) 상태: %s",
                          node.get("name"), node.get("host"), node.get("port"), status)

        # 현재 leader 확인
        current_leader = get_current_leader(nodes)
        if current_leader:
            if current_leader.get("healthy"):
                logging.debug("현재 leader는 %s", current_leader.get("name"))
            else:
                logging.warning("현재 leader %s가 비정상입니다.", current_leader.get("name"))
        else:
            logging.warning("현재 leader가 지정되어 있지 않습니다.")

        # 건강한 노드 중 failover_order가 가장 낮은 노드를 찾음
        best_candidate = get_best_candidate(nodes)

        promotion_needed = False
        reason = ""

        if not current_leader or not current_leader.get("healthy"):
            if best_candidate:
                promotion_needed = True
                reason = "현재 leader 비정상"
            else:
                logging.error("모든 노드가 비정상입니다. 프로모션 불가")
        else:
            # failback: 현재 leader보다 우선순위가 더 높은 건강한 노드가 존재하는 경우
            if failback_enabled and best_candidate:
                if best_candidate.get("failover_order") < current_leader.get("failover_order"):
                    if current_time - last_promotion_time >= promotion_cooldown:
                        promotion_needed = True
                        reason = "failback: 우선순위가 더 높은 노드가 건강함"
                    else:
                        logging.debug("프로모션 쿨다운 중 (남은 시간: %s초)",
                                      promotion_cooldown - (current_time - last_promotion_time))
        if promotion_needed and best_candidate:
            logging.info("프로모션 시작 (사유: %s). 후보 노드: %s", reason, best_candidate.get("name"))
            success = promote_node(best_candidate, promotion_command)
            if success:
                # 역할 업데이트
                update_roles(nodes, best_candidate.get("name"))
                last_promotion_time = current_time

                # 다른 노드들에 대해 복제 재구성 실행
                for node in nodes:
                    if node.get("name") != best_candidate.get("name") and node.get("healthy"):
                        reconfigure_replication(best_candidate, node, replication_command)
            else:
                logging.error("프로모션 실패. 다음 체크 주기까지 대기합니다.")
        else:
            logging.debug("프로모션 불필요: 현재 leader 유지")

        time.sleep(check_interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("프로그램 종료됨 (KeyboardInterrupt)")
        sys.exit(0)

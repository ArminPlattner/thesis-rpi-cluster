"""
Realistic Ansible Log Dataset Generator  (v3 — expanded blueprints)
=====================================================================
Key improvements over v2:
  • 10-12 blueprints per IaC category (up from 4-5)
  • Per-category format weights: Syntax/Documentation use error_global_* 
    formats; runtime categories (Service, Security, Dependency) use 
    failed_task/failed_inline so format alone doesn't signal category
  • Unique skeleton rate target: >25% per category (vs ~5-12% in v2)
  • Real messages from Ansible_errors_raw.csv still used exactly once each
"""

import json
import random
import pandas as pd
from sklearn.model_selection import train_test_split

random.seed(2025)

HOSTS = [
    # Web tier
    "web-node-01", "web-node-02", "web-node-03", "web-node-04",
    "web-frontend-eu-01", "web-frontend-us-01",
    # Database tier
    "db-srv-main-01", "db-srv-replica-01", "db-srv-replica-02",
    "db-postgres-primary", "db-mysql-replica-02", "db-mongo-shard-01",
    # Kubernetes workers
    "app-k3s-worker-01", "app-k3s-worker-02", "app-k3s-worker-03",
    "app-k3s-worker-04", "app-k3s-worker-05",
    "kube-node-prod-01", "kube-node-prod-02", "kube-node-staging-01",
    # Load balancers / ingress
    "lb-haproxy", "lb-external-ingress", "lb-internal-mesh",
    "ingress-nginx-01", "ingress-traefik-prod",
    # Storage
    "storage-block-01", "storage-nas", "storage-ceph-osd-01",
    "storage-nfs-srv",
    # API / gateway
    "api-gateway-node-01", "api-gateway-node-02", "api-gateway-node-03",
    # Caches
    "redis-cache-01", "redis-cache-02", "memcache-01",
    # CI/CD
    "ci-runner-deploy-01", "ci-runner-build-01",
    "jenkins-agent-01", "gitlab-runner-02",
    # Monitoring
    "monitor-prometheus-01", "monitor-grafana-01",
    "monitor-loki-01", "monitor-alertmanager-01",
    # Security / secrets
    "vault-server-01", "vault-server-02",
    "bastion-host-prod",
]

PATHS = [
    "playbooks/deploy-app.yml", "playbooks/provision-infra.yml",
    "playbooks/database-setup.yml", "playbooks/security-hardening.yml",
    "playbooks/rollback.yml", "playbooks/certificates.yml",
    "playbooks/bootstrap-cluster.yml", "playbooks/drain-node.yml",
    "roles/common/tasks/main.yml", "roles/common/handlers/main.yml",
    "roles/common/vars/main.yml",
    "roles/web/tasks/configure.yml", "roles/web/tasks/install.yml",
    "roles/web/templates/nginx.conf.j2", "roles/web/templates/vhost.conf.j2",
    "roles/db/tasks/initialize.yml", "roles/db/tasks/secure.yml",
    "roles/db/templates/my.cnf.j2", "roles/db/templates/pg_hba.conf.j2",
    "roles/app/tasks/deploy.yml", "roles/app/tasks/runtime.yml",
    "roles/app/templates/app.conf.j2",
    "roles/security/tasks/firewall.yml", "roles/security/tasks/users.yml",
    "roles/security/tasks/certificates.yml", "roles/security/tasks/ssh.yml",
    "roles/monitoring/tasks/metrics.yml", "roles/monitoring/tasks/alerts.yml",
    "roles/monitoring/templates/prometheus.yml.j2",
    "roles/network/tasks/routes.yml", "roles/network/tasks/dns.yml",
    "roles/storage/tasks/volumes.yml", "roles/storage/tasks/mounts.yml",
    "roles/k8s/tasks/deploy.yml", "roles/k8s/tasks/namespaces.yml",
    "group_vars/all.yml", "group_vars/webservers.yml",
    "group_vars/dbservers.yml", "group_vars/k8s_workers.yml",
    "host_vars/web-node-01.yml", "host_vars/db-srv-main-01.yml",
    "host_vars/app-k3s-worker-01.yml",
]

PRECEDING_OK_TASKS = [
    ("Gathering Facts", "ok"),
    ("Ensure base packages are present", "ok"),
    ("Create application directory", "changed"),
    ("Copy configuration template", "changed"),
    ("Set file ownership", "ok"),
    ("Verify target directory exists", "ok"),
    ("Flush pending handlers", "ok"),
    ("Check service registration", "ok"),
    ("Validate network interface", "ok"),
    ("Reload systemd unit files", "changed"),
    ("Install required Python packages", "changed"),
    ("Configure firewall rules", "changed"),
    ("Deploy SSL certificate", "changed"),
    ("Synchronise NTP", "ok"),
    ("Register node in inventory", "changed"),
    ("Run pre-flight health checks", "ok"),
    # Additional variety
    ("Pull container image", "changed"),
    ("Apply Kubernetes manifests", "changed"),
    ("Update apt package cache", "changed"),
    ("Create system group", "changed"),
    ("Write environment variables", "changed"),
    ("Ensure log directory exists", "ok"),
    ("Configure rsyslog forwarding", "changed"),
    ("Validate TLS certificate expiry", "ok"),
    ("Drain Kafka consumer offsets", "ok"),
    ("Snapshot volume before migration", "changed"),
    ("Verify cluster quorum", "ok"),
    ("Rotate log files", "changed"),
    ("Check disk free space", "ok"),
    ("Ping target hosts", "ok"),
    ("Load role-specific variables", "ok"),
    ("Create tmpfs mount", "changed"),
]

STARS = "*" * 54

ITEMS = [
    "nginx", "postgresql", "redis", "k3s", "prometheus",
    "node-exporter", "haproxy", "vault", "consul", "etcd",
    "alertmanager", "grafana", "certbot", "logrotate",
    # Additional service names
    "kafka", "zookeeper", "elasticsearch", "kibana", "logstash",
    "rabbitmq", "memcached", "mysql", "mongodb", "cassandra",
    "traefik", "envoy", "istio-proxy", "fluentd", "jaeger",
]
VARS = [
    "db_password", "api_endpoint", "cluster_token", "vault_addr",
    "registry_url", "replica_count", "namespace", "tls_cert_path",
    "backup_bucket", "log_level", "bind_address", "secret_key",
    "smtp_password", "oauth_client_id", "s3_bucket_name",
    # Additional variable names
    "admin_token", "replication_factor", "retention_days",
    "max_heap_size", "node_labels", "ingress_class", "storage_class",
    "pull_secret", "image_tag", "chart_version", "kubeconfig_path",
    "ssl_cert_cn", "ldap_bind_dn", "prometheus_scrape_interval",
]
PKGS = [
    "python3-cryptography", "libpq-dev", "jmespath", "netaddr",
    "kubernetes", "boto3", "hvac", "pyopenssl", "requests-kerberos",
    "ansible-lint", "molecule", "passlib", "selinux",
    # Additional packages
    "python3-psutil", "docker-py", "openshift", "pyzmq",
    "python-ldap", "mysql-connector-python", "pymongo",
    "google-auth", "azure-mgmt-compute", "pyvmomi",
]
PORTS = [
    5432, 6379, 8080, 9090, 9100, 3306, 443, 2379, 8443, 27017,
    6443, 10250, 2380, 4001,
    # Additional ports
    9200, 5601, 5672, 15672, 9092, 2181, 6380, 8888, 3000,
    9093, 4444, 8086, 2003, 8125,
]
USERS = [
    "deploy", "ansible", "prometheus", "vault", "postgres",
    "nginx", "redis", "grafana", "alertmanager",
    # Additional users
    "kafka", "elasticsearch", "kibana", "rabbitmq", "mongodb",
    "jenkins", "gitlab-runner", "fluentd", "consul", "etcd",
]
MODULES = [
    "ansible.builtin.systemd", "ansible.builtin.apt",
    "ansible.builtin.command", "ansible.builtin.template",
    "ansible.builtin.copy", "ansible.builtin.uri",
    "ansible.builtin.user", "ansible.builtin.lineinfile",
    "ansible.builtin.iptables", "ansible.builtin.wait_for",
    "community.general.ini_file", "community.mysql.mysql_variables",
    # Additional modules for variety in error messages
    "ansible.builtin.pip", "ansible.builtin.git",
    "ansible.builtin.cron", "ansible.builtin.set_fact",
    "ansible.posix.sysctl", "community.docker.docker_container",
    "kubernetes.core.k8s", "ansible.builtin.assert",
]


def _fill(template: str, host: str) -> str:
    try:
        return template.format(
            host=host, path=random.choice(PATHS),
            line=random.randint(3, 280), col=random.randint(1, 16),
            item=random.choice(ITEMS), var=random.choice(VARS),
            pkg=random.choice(PKGS), port=random.choice(PORTS),
            user=random.choice(USERS), uid=random.randint(1000, 65534),
            rc=random.choice([1, 2, 126, 127, 255]),
            module=random.choice(MODULES),
        )
    except KeyError:
        return template


def _preceding(task_names: list, host: str) -> list:
    lines = []
    for name in task_names:
        status = random.choice(["ok", "changed"])
        lines += [f"TASK [{name}] {STARS}", f"{status}: [{host}]", ""]
    return lines


def render_fatal(msg: str, module: str | None, host: str, fmt: str) -> str:
    path = random.choice(PATHS)
    line = random.randint(3, 280)
    col  = random.randint(1, 16)
    if fmt == "failed_task":
        rc = random.choice([1, 2, 126, 127, 255])
        result = {"changed": False, "msg": msg, "rc": rc}
        if module:
            result["invocation"] = {"module_args": {
                "name": random.choice(ITEMS), "state": "started"}}
        task = random.choice(PRECEDING_OK_TASKS)[0]
        return f"TASK [{task}] {STARS}\nfatal: [{host}]: FAILED! => {json.dumps(result)}"
    elif fmt == "failed_inline":
        result = {"changed": False, "msg": msg,
                  "ansible_loop_var": "item", "item": random.choice(ITEMS)}
        return f"failed: [{host}] (item={random.choice(ITEMS)}) => {json.dumps(result)}"
    elif fmt == "unreachable":
        result = {"changed": False, "msg": msg, "unreachable": True}
        task = random.choice(PRECEDING_OK_TASKS)[0]
        return f"TASK [{task}] {STARS}\nfatal: [{host}]: UNREACHABLE! => {json.dumps(result)}"
    elif fmt == "error_global_verbose":
        return f"ERROR! {msg}\n\nThe error appears to be in '{path}': line {line}, column {col}"
    elif fmt == "error_global_contextual":
        return (f"ERROR! {msg}\n\nThe error appears to have been in '{path}': line {line}, "
                f"column {col}, but may\nbe elsewhere in the file depending on the exact "
                f"syntax problem.")
    else:  # error_global_simple
        return f"ERROR! {msg}"


# ─────────────────────────────────────────────────────────────────────────────
# BLUEPRINT POOL  —  (preceding_tasks, module_or_None, msg_template,
#                     fault_category, allowed_formats)
#
# allowed_formats drives which render path is chosen.  Runtime categories
# (Security, Service, Dependency, Idempotency) are biased toward failed_task /
# failed_inline so format alone cannot signal the category.  Syntax /
# Documentation are biased toward error_global_* because that is how Ansible
# actually surfaces those errors, making the dataset realistic.
# ─────────────────────────────────────────────────────────────────────────────

BLUEPRINTS: dict[str, list] = {

"Security": [
    (["Configure SSH authorised keys", "Deploy TLS certificate"],
     "ansible.builtin.authorized_key",
     "Failed to read key material from {path}: permission denied (uid={uid})",
     "State Mismanagement",
     ["failed_task", "failed_task", "failed_task", "failed_inline"]),

    (["Create system user account", "Set sudo policy"],
     "ansible.builtin.user",
     "Privilege escalation check failed for account '{user}': sudoers entry absent",
     "Incorrect Detection/Analysis of User Input",
     ["failed_task", "failed_task", "failed_inline"]),

    (["Validate vault-encrypted variable", "Distribute secrets bundle"],
     "community.general.ini_file",
     "Unable to decrypt vault payload — check ANSIBLE_VAULT_PASSWORD_FILE",
     "Variable Misreference",
     ["failed_task", "error_global_verbose", "error_global_simple"]),

    (["Harden SSH daemon config", "Reload SSHD"],
     "ansible.builtin.lineinfile",
     "Regex anchor matched zero lines in {path}; expected exactly one substitution",
     "State Mismanagement",
     ["failed_task", "failed_task", "failed_inline"]),

    (["Rotate API token", "Invalidate old session"],
     "ansible.builtin.uri",
     "HTTP 403 received from auth endpoint — credentials may have been rotated already",
     "Incorrect Detection/Analysis of User Input",
     ["failed_task", "failed_task", "failed_inline"]),

    (["Deploy SSH authorised_keys", "Validate key fingerprint"],
     None,
     "Host key verification failed for {host}: REMOTE HOST IDENTIFICATION HAS CHANGED",
     "State Mismanagement",
     ["error_global_verbose", "error_global_simple", "failed_task"]),

    (["Generate client certificate", "Push to secrets store"],
     "ansible.builtin.openssl_certificate",
     "Certificate validation error: subject CN does not match expected hostname '{host}'",
     "Incorrect Detection/Analysis of User Input",
     ["failed_task", "failed_inline"]),

    (["Apply SELinux policy", "Restore file contexts"],
     "ansible.builtin.command",
     "semanage fcontext returned rc={rc}: policy module not loaded for path {path}",
     "State Mismanagement",
     ["failed_task", "failed_task", "failed_inline"]),

    (["Create service account", "Bind RBAC role"],
     "ansible.builtin.user",
     "Account '{user}' already exists with conflicting UID; reconciliation skipped",
     "Idempotency",
     ["failed_task", "failed_inline"]),

    (["Audit sudoers entries", "Remove legacy accounts"],
     "ansible.builtin.lineinfile",
     "Sudoers syntax check failed after edit: visudo reported parse error near line {line}",
     "Typos",
     ["failed_task", "error_global_verbose"]),

    (["Distribute internal CA certificate", "Update trust store"],
     "ansible.builtin.copy",
     "Destination {path} is not writable by uid={uid}; become escalation did not apply",
     "State Mismanagement",
     ["failed_task", "failed_task"]),
],

"Service": [
    (["Install service package", "Enable systemd unit"],
     "ansible.builtin.systemd",
     "Unit {item}.service not found in systemd journal — confirm package installation",
     "Dependency-related Faults",
     ["failed_task", "failed_task", "failed_inline"]),

    (["Write service drop-in override", "Reload daemon"],
     "ansible.builtin.systemd",
     "Failed to start {item}.service: exit code {rc} — inspect journalctl for detail",
     "State Mismanagement",
     ["failed_task", "failed_task", "failed_inline"]),

    (["Register service with consul", "Check port binding"],
     "ansible.builtin.wait_for",
     "Timeout waiting for port {port}/tcp to become reachable on {host}",
     "State Mismanagement",
     ["failed_task", "failed_inline"]),

    (["Deploy systemd socket unit", "Activate socket"],
     "ansible.builtin.command",
     "Command returned non-zero: rc={rc}; stderr: activation request refused",
     "Incorrect Detection/Analysis of User Input",
     ["failed_task", "failed_inline"]),

    (["Check systemd service health", "Trigger remediation"],
     "ansible.builtin.systemd",
     "Service {item} entered failed state before readiness probe completed",
     "State Mismanagement",
     ["failed_task", "failed_task", "failed_inline"]),

    (["Configure load balancer backend", "Validate upstream health"],
     "ansible.builtin.uri",
     "Upstream {host}:{port} returned HTTP {rc}; backend removed from rotation",
     "State Mismanagement",
     ["failed_task", "failed_inline"]),

    (["Deploy containerised service", "Wait for pod readiness"],
     "ansible.builtin.command",
     "kubectl rollout status timed out: deployment/{item} not ready after 300s",
     "Dependency-related Faults",
     ["failed_task", "error_global_simple"]),

    (["Configure cron job", "Validate schedule expression"],
     "ansible.builtin.cron",
     "Cron entry for user {user} rejected: schedule '{item}' failed validation",
     "Incorrect Detection/Analysis of User Input",
     ["failed_task", "failed_inline"]),

    (["Install apt package", "Pin version"],
     "ansible.builtin.apt",
     "dpkg was interrupted; run 'dpkg --configure -a' to correct the problem",
     "State Mismanagement",
     ["failed_task", "error_global_simple"]),

    (["Ensure rsyslog is running", "Configure remote logging"],
     "ansible.builtin.systemd",
     "rsyslog.service masked; cannot start masked unit",
     "State Mismanagement",
     ["failed_task", "failed_task"]),
],

"Dependency": [
    (["Update apt cache", "Install runtime dependencies"],
     "ansible.builtin.apt",
     "Package '{pkg}' has no installation candidate in the current apt sources",
     "Dependency-related Faults",
     ["failed_task", "failed_task", "failed_inline"]),

    (["Set up Python virtualenv", "Install pip requirements"],
     "ansible.builtin.pip",
     "Could not find a version that satisfies the requirement {pkg}",
     "Dependency-related Faults",
     ["failed_task", "failed_inline"]),

    (["Clone application repository", "Checkout release branch"],
     "ansible.builtin.git",
     "Failed to resolve remote ref '{var}' — branch or tag may not exist",
     "Variable Misreference",
     ["failed_task", "failed_inline"]),

    (["Install Ansible collection", "Load collection namespace"],
     "ansible.builtin.command",
     "Namespace collision detected: collection '{pkg}' shadows a built-in module",
     "Dependency-related Faults",
     ["failed_task", "error_global_verbose", "error_global_simple"]),

    (["Verify Python interpreter path", "Load module"],
     None,
     "ImportError: cannot import name '{var}' from '{pkg}'; installed version may be incompatible",
     "Dependency-related Faults",
     ["error_global_verbose", "error_global_simple", "failed_task"]),

    (["Install OS-level library", "Link shared objects"],
     "ansible.builtin.apt",
     "Dependency tree resolution failed: {pkg} conflicts with already-installed {pkg}",
     "Dependency-related Faults",
     ["failed_task", "failed_task"]),

    (["Pull Helm chart", "Resolve chart dependencies"],
     "ansible.builtin.command",
     "Error: chart '{item}' not found in repository; run 'helm repo update' first",
     "Dependency-related Faults",
     ["failed_task", "error_global_simple"]),

    (["Install Galaxy role", "Validate role metadata"],
     None,
     "The role '{item}' was not found in any of the configured role sources",
     "Dependency-related Faults",
     ["error_global_verbose", "error_global_simple"]),

    (["Fetch container image", "Authenticate registry"],
     "ansible.builtin.command",
     "manifest unknown: manifest tagged '{var}' not found in registry",
     "Variable Misreference",
     ["failed_task", "failed_inline"]),

    (["Validate Ansible version constraint", "Abort if unsatisfied"],
     None,
     "This role requires ansible >= 2.14; detected {var}. Upgrade your controller.",
     "Dependency-related Faults",
     ["error_global_simple", "error_global_verbose"]),
],

"Configuration Data": [
    (["Render Jinja2 template", "Write config to target"],
     "ansible.builtin.template",
     "AnsibleUndefinedVariable: '{var}' is undefined — verify group_vars/host_vars",
     "Variable Misreference",
     ["error_global_verbose", "failed_task", "error_global_simple"]),

    (["Load host-specific vars file", "Validate IP block range"],
     "ansible.builtin.set_fact",
     "Expected a valid CIDR string for key 'network_cidr'; received: '{var}'",
     "Incorrect Detection/Analysis of User Input",
     ["failed_task", "failed_inline"]),

    (["Write database DSN to environment", "Restart application"],
     "ansible.builtin.lineinfile",
     "Destination file {path} does not exist and create=false; no changes made",
     "State Mismanagement",
     ["failed_task", "failed_task"]),

    (["Set MySQL bind address", "Flush privileges"],
     "community.mysql.mysql_variables",
     "Variable 'max_connections' rejected: value {port} exceeds compiled-in limit",
     "Incorrect Detection/Analysis of User Input",
     ["failed_task", "failed_inline"]),

    (["Populate inventory from CMDB", "Validate group membership"],
     None,
     "No hosts matched pattern '{var}' in inventory — check group name spelling",
     "Incorrect Detection/Analysis of User Input",
     ["error_global_verbose", "error_global_simple"]),

    (["Write nginx upstream config", "Reload service"],
     "ansible.builtin.template",
     "Template rendering failed: recursive variable reference detected at '{var}'",
     "Variable Misreference",
     ["failed_task", "error_global_verbose"]),

    (["Validate JSON config payload", "Push to API"],
     "ansible.builtin.uri",
     "JSON body parse error: unexpected token at position {col} in field '{var}'",
     "Typos",
     ["failed_task", "failed_inline"]),

    (["Read environment-specific vars", "Override defaults"],
     None,
     "vars file '{path}' is not a valid YAML dictionary; expected mapping at top level",
     "Typos",
     ["error_global_verbose", "error_global_simple"]),

    (["Set cluster advertise address", "Bootstrap etcd"],
     "ansible.builtin.set_fact",
     "IPv6 address '{var}' used where IPv4 is required by module parameter 'bind_addr'",
     "Incorrect Detection/Analysis of User Input",
     ["failed_task", "failed_task"]),

    (["Write prometheus scrape config", "Reload alertmanager"],
     "ansible.builtin.template",
     "YAML anchor '&{var}' referenced before definition in template {path}",
     "Variable Misreference",
     ["failed_task", "error_global_verbose"]),
],

"Idempotency": [
    (["Create log rotation entry", "Write cron job"],
     "ansible.builtin.cron",
     "Duplicate entry detected in crontab for user {user}; idempotency guard triggered",
     "State Mismanagement",
     ["failed_task", "failed_task", "failed_inline"]),

    (["Initialise database schema", "Apply migration script"],
     "ansible.builtin.command",
     "Schema object '{item}' already exists; re-run without --force to skip safely",
     "Incorrect Task Iteration Logic",
     ["failed_task", "failed_inline"]),

    (["Register node with inventory", "Tag resource in CMDB"],
     "ansible.builtin.uri",
     "Resource already registered under a different UUID; cannot create duplicate",
     "State Mismanagement",
     ["failed_task", "failed_task"]),

    (["Add firewall rule", "Commit ruleset"],
     "ansible.builtin.iptables",
     "Rule already present in chain {item}; insertion skipped but exit status non-zero",
     "Incorrect Task Iteration Logic",
     ["failed_task", "failed_inline"]),

    (["Create system user", "Set shell and home dir"],
     "ansible.builtin.user",
     "User '{user}' already exists with home={path}; conflicting uid prevents idempotent update",
     "State Mismanagement",
     ["failed_task", "failed_inline"]),

    (["Write TLS certificate to disk", "Set permissions"],
     "ansible.builtin.copy",
     "File {path} already exists with different content; force=no prevents overwrite",
     "State Mismanagement",
     ["failed_task", "failed_task"]),

    (["Run database init script", "Seed reference data"],
     "ansible.builtin.command",
     "Table '{var}' already populated; seed script exited with rc={rc} to signal no-op",
     "Incorrect Task Iteration Logic",
     ["failed_task", "failed_inline"]),

    (["Configure sysctl parameter", "Apply kernel setting"],
     "ansible.posix.sysctl",
     "sysctl key '{var}' already set to target value; changed=false but rc={rc} unexpected",
     "State Mismanagement",
     ["failed_task", "failed_task"]),

    (["Add apt repository key", "Update cache"],
     "ansible.builtin.apt_key",
     "Key with fingerprint already exists in keyring; duplicate import returned error",
     "Incorrect Task Iteration Logic",
     ["failed_task", "failed_inline"]),

    (["Create Kubernetes namespace", "Apply labels"],
     "ansible.builtin.command",
     "namespace/{item} already exists; kubectl apply returned non-zero rc={rc}",
     "State Mismanagement",
     ["failed_task", "failed_task"]),
],

"Conditional": [
    (["Check OS family", "Apply OS-specific role"],
     "ansible.builtin.include_tasks",
     "Condition evaluated to unexpected type bool({var}); task branch not entered",
     "Incorrect Detection/Analysis of User Input",
     ["failed_task", "failed_inline"]),

    (["Assert minimum kernel version", "Proceed with module load"],
     "ansible.builtin.assert",
     "Assertion failed: required kernel >= 5.15, detected {var} on {host}",
     "Incorrect Detection/Analysis of User Input",
     ["failed_task", "failed_task", "failed_inline"]),

    (["Check whether service is healthy", "Conditionally restart"],
     "ansible.builtin.fail",
     "Explicit failure triggered: health-check probe returned status '{item}'",
     "State Mismanagement",
     ["failed_task", "failed_inline"]),

    (["Evaluate deployment gate", "Continue or abort pipeline"],
     "ansible.builtin.debug",
     "Variable '{var}' resolved to None; downstream when-clause will always skip",
     "Variable Misreference",
     ["error_global_verbose", "failed_task"]),

    (["Run block with rescue handler", "Execute cleanup on error"],
     None,
     "Block rescue executed but rescue task also failed: unhandled exception in handler for '{item}'",
     "Incorrect Task Iteration Logic",
     ["error_global_verbose", "error_global_simple"]),

    (["Check disk free space", "Abort if below threshold"],
     "ansible.builtin.assert",
     "Assert 'ansible_mounts[0].size_available > {port}' evaluated False on {host}",
     "State Mismanagement",
     ["failed_task", "failed_inline"]),

    (["Inspect running container list", "Branch on presence"],
     "ansible.builtin.command",
     "when: clause references '{var}' which was never registered; defaulting to False",
     "Variable Misreference",
     ["error_global_verbose", "failed_task"]),

    (["Test connectivity before proceeding", "Skip unreachable hosts"],
     "ansible.builtin.wait_for_connection",
     "all_hosts_matching pattern evaluated to empty set; no tasks will execute",
     "Incorrect Detection/Analysis of User Input",
     ["error_global_simple", "failed_task"]),

    (["Validate environment tag", "Skip non-production"],
     "ansible.builtin.include_role",
     "Role guard: expected env=production, got '{var}'; role skipped but exit non-zero",
     "Incorrect Detection/Analysis of User Input",
     ["failed_task", "failed_inline"]),

    (["Count running replicas", "Scale if below minimum"],
     "ansible.builtin.command",
     "Registered variable '{var}' has no attribute 'stdout_lines'; check task output",
     "Variable Misreference",
     ["error_global_verbose", "error_global_simple"]),
],

"Syntax": [
    (["Parse playbook structure"],
     None,
     "Syntax error in {path} at line {line}, col {col}: mapping values are not allowed here",
     "Typos",
     ["error_global_contextual", "error_global_verbose"]),

    (["Load role defaults"],
     None,
     "YAML parse error in {path}: found duplicate key '{var}' (line {line})",
     "Typos",
     ["error_global_verbose", "error_global_contextual"]),

    (["Validate task parameters"],
     None,
     "Parameters 'creates' and 'removes' are mutually exclusive in module ansible.builtin.command",
     "Incorrect Task Iteration Logic",
     ["error_global_simple", "error_global_verbose"]),

    (["Evaluate module argument spec"],
     None,
     "Unsupported parameter for module '{module}': '{var}'. Supported params: name, state, enabled",
     "Typos",
     ["error_global_simple", "error_global_contextual"]),

    (["Tokenise Jinja2 expression"],
     None,
     "Jinja2 TemplateSyntaxError: unexpected end of template at line {line} in {path}",
     "Typos",
     ["error_global_verbose", "error_global_contextual"]),

    (["Validate loop variable binding"],
     None,
     "loop variable '{var}' shadows an existing variable; behaviour undefined under free strategy",
     "Incorrect Task Iteration Logic",
     ["error_global_simple", "error_global_verbose"]),

    (["Check task argument types"],
     None,
     "The field 'retries' is expected to be an integer but got string: '{var}'",
     "Typos",
     ["error_global_simple", "error_global_contextual"]),

    (["Parse host pattern expression"],
     None,
     "Invalid host pattern '{var}': range syntax requires two integers separated by ':'",
     "Typos",
     ["error_global_verbose", "error_global_simple"]),

    (["Validate notify handler name"],
     None,
     "Handler '{item}' referenced in notify but no matching handler defined in play",
     "Typos",
     ["error_global_verbose", "error_global_simple"]),

    (["Check include_tasks path"],
     None,
     "No file was found when looking for '{path}'; check spelling and roles_path",
     "Typos",
     ["error_global_contextual", "error_global_simple"]),

    (["Inspect block structure"],
     None,
     "A block entry must contain at least one task; empty block at line {line} in {path}",
     "Incorrect Task Iteration Logic",
     ["error_global_verbose", "error_global_contextual"]),
],

"Documentation": [
    (["Load role metadata", "Validate galaxy dependencies"],
     "ansible.builtin.include_role",
     "Role '{item}' listed in meta/main.yml not found in configured roles_path",
     "Dependency-related Faults",
     ["error_global_verbose", "error_global_simple"]),

    (["Read inline task documentation", "Enforce naming convention"],
     None,
     "Task name exceeds 120 chars or contains unsupported Unicode in {path} line {line}",
     "Typos",
     ["error_global_simple", "error_global_verbose"]),

    (["Apply changelog annotation", "Tag release commit"],
     "ansible.builtin.command",
     "Changelog entry references non-existent issue #{item}; CI gate rejected the run",
     "Incorrect Detection/Analysis of User Input",
     ["failed_task", "error_global_simple"]),

    (["Check galaxy meta format", "Validate author field"],
     None,
     "galaxy.yml is missing required key 'authors'; role will not pass galaxy import",
     "Typos",
     ["error_global_verbose", "error_global_simple"]),

    (["Validate README structure", "Check example playbook"],
     None,
     "README.md references variable '{var}' not declared in defaults/main.yml",
     "Variable Misreference",
     ["error_global_simple", "error_global_verbose"]),

    (["Lint task descriptions", "Check for deprecated keywords"],
     None,
     "Deprecated keyword 'always_run' found in {path} line {line}; use 'become' instead",
     "Typos",
     ["error_global_contextual", "error_global_verbose"]),

    (["Parse CHANGELOG.md", "Assert semantic version format"],
     None,
     "Version string '{var}' in CHANGELOG does not follow SemVer; release pipeline blocked",
     "Typos",
     ["error_global_simple", "error_global_verbose"]),

    (["Check module documentation block", "Validate example syntax"],
     None,
     "DOCUMENTATION block in {path} is not valid YAML; ansible-doc will fail for this module",
     "Typos",
     ["error_global_verbose", "error_global_contextual"]),

    (["Enforce task tagging policy", "Check coverage"],
     None,
     "Task at line {line} in {path} has no tags assigned; tagging policy requires at least one",
     "Incorrect Detection/Analysis of User Input",
     ["error_global_verbose", "error_global_simple"]),
],

}  # end BLUEPRINTS


# ── Category taxonomy for real CSV messages (unchanged from v2) ───────────────
REAL_MSG_TAXONOMY = [
    ("winrm ssl",             "Security",           "State Mismanagement"),
    ("winrm",                 "Security",           "Dependency-related Faults"),
    ("ssh host key",          "Security",           "State Mismanagement"),
    ("ssh permission",        "Security",           "Incorrect Detection/Analysis of User Input"),
    ("ssh connection",        "Security",           "State Mismanagement"),
    ("ssh invalid key",       "Security",           "Typos"),
    ("ssh agent",             "Security",           "State Mismanagement"),
    ("sudo requires",         "Security",           "Incorrect Detection/Analysis of User Input"),
    ("become method",         "Security",           "Incorrect Detection/Analysis of User Input"),
    ("become user",           "Security",           "Incorrect Detection/Analysis of User Input"),
    ("missing sudo",          "Security",           "Incorrect Detection/Analysis of User Input"),
    ("vault password not",    "Security",           "Incorrect Detection/Analysis of User Input"),
    ("vault decryption",      "Security",           "State Mismanagement"),
    ("not vault encrypted",   "Security",           "Typos"),
    ("multiple vault",        "Security",           "Incorrect Detection/Analysis of User Input"),
    ("vault editor",          "Security",           "Variable Misreference"),
    ("inline vault",          "Security",           "Variable Misreference"),
    ("extra newline in vault","Security",            "Typos"),
    ("vault password file",   "Security",           "Incorrect Detection/Analysis of User Input"),
    ("file module permission","Security",            "State Mismanagement"),
    ("module not found",      "Dependency",         "Dependency-related Faults"),
    ("conda init",            "Dependency",         "Dependency-related Faults"),
    ("role not found",        "Dependency",         "Dependency-related Faults"),
    ("galaxy install",        "Dependency",         "Dependency-related Faults"),
    ("role dependency",       "Dependency",         "Dependency-related Faults"),
    ("collection not",        "Dependency",         "Dependency-related Faults"),
    ("role tasks",            "Dependency",         "Dependency-related Faults"),
    ("galaxy requirements",   "Dependency",         "Typos"),
    ("galaxy api",            "Dependency",         "State Mismanagement"),
    ("boto3",                 "Dependency",         "Dependency-related Faults"),
    ("docker sdk",            "Dependency",         "Dependency-related Faults"),
    ("kubernetes python",     "Dependency",         "Dependency-related Faults"),
    ("python interpreter",    "Dependency",         "Dependency-related Faults"),
    ("python version",        "Dependency",         "Dependency-related Faults"),
    ("cryptography module",   "Dependency",         "Dependency-related Faults"),
    ("jinja2 version",        "Dependency",         "Dependency-related Faults"),
    ("pyyaml",                "Dependency",         "Dependency-related Faults"),
    ("pkg_resources",         "Dependency",         "Dependency-related Faults"),
    ("ansible not found",     "Dependency",         "Dependency-related Faults"),
    ("paramiko",              "Dependency",         "Dependency-related Faults"),
    ("ssh controlpersist",    "Dependency",         "Dependency-related Faults"),
    ("connection refused",    "Service",            "State Mismanagement"),
    ("connection timeout",    "Service",            "State Mismanagement"),
    ("host unreachable",      "Service",            "State Mismanagement"),
    ("delegate_to host",      "Service",            "State Mismanagement"),
    ("apt lock",              "Service",            "State Mismanagement"),
    ("yum gpg",               "Service",            "State Mismanagement"),
    ("service not found",     "Service",            "Dependency-related Faults"),
    ("async task",            "Service",            "State Mismanagement"),
    ("retries exceeded",      "Service",            "State Mismanagement"),
    ("fact gathering",        "Service",            "State Mismanagement"),
    ("too many open",         "Service",            "State Mismanagement"),
    ("inventory parse",       "Configuration Data", "Typos"),
    ("duplicate host",        "Configuration Data", "Typos"),
    ("no inventory",          "Configuration Data", "Incorrect Detection/Analysis of User Input"),
    ("inventory variable",    "Configuration Data", "Variable Misreference"),
    ("dynamic inventory",     "Configuration Data", "State Mismanagement"),
    ("host not found",        "Configuration Data", "Incorrect Detection/Analysis of User Input"),
    ("no hosts matched",      "Configuration Data", "Incorrect Detection/Analysis of User Input"),
    ("children group",        "Configuration Data", "Incorrect Detection/Analysis of User Input"),
    ("vars file",             "Configuration Data", "Incorrect Detection/Analysis of User Input"),
    ("extra vars json",       "Configuration Data", "Typos"),
    ("hostvars undefined",    "Configuration Data", "Variable Misreference"),
    ("handler not found",     "Configuration Data", "State Mismanagement"),
    ("template destination",  "Configuration Data", "State Mismanagement"),
    ("template unicode",      "Configuration Data", "Incorrect Detection/Analysis of User Input"),
    ("template unwanted",     "Configuration Data", "State Mismanagement"),
    ("copy module checksum",  "Configuration Data", "State Mismanagement"),
    ("playbook missing",      "Configuration Data", "Incorrect Detection/Analysis of User Input"),
    ("invalid serial",        "Configuration Data", "Typos"),
    ("callback plugin",       "Configuration Data", "State Mismanagement"),
    ("include tasks",         "Configuration Data", "Dependency-related Faults"),
    ("jinja2 regex",          "Conditional",        "Typos"),
    ("when conditional",      "Conditional",        "Typos"),
    ("block/rescue",          "Conditional",        "Typos"),
    ("free strategy",         "Conditional",        "Incorrect Task Iteration Logic"),
    ("command module always", "Idempotency",        "Incorrect Task Iteration Logic"),
    ("lineinfile regex",      "Idempotency",        "Incorrect Task Iteration Logic"),
    ("yaml syntax",           "Syntax",             "Typos"),
    ("playbook yaml",         "Syntax",             "Typos"),
    ("duplicate key",         "Syntax",             "Typos"),
    ("no action detected",    "Syntax",             "Typos"),
    ("jinja2 template syntax","Syntax",             "Typos"),
    ("jinja2 syntax",         "Syntax",             "Typos"),
    ("jinja2 filter",         "Syntax",             "Typos"),
    ("nested loop variable",  "Syntax",             "Incorrect Task Iteration Logic"),
    ("all tasks skipped",     "Syntax",             "Incorrect Task Iteration Logic"),
    ("task does not support", "Syntax",             "Incorrect Task Iteration Logic"),
    ("boolean variable deprec","Documentation",     "Typos"),
    ("output hidden",         "Documentation",      "Incorrect Detection/Analysis of User Input"),
    ("galaxy requirements format","Documentation",  "Typos"),
]


def map_real_message(error_type: str) -> tuple[str, str]:
    et = error_type.lower()
    for key, iac, fault in REAL_MSG_TAXONOMY:
        if key in et:
            return iac, fault
    return "Configuration Data", "State Mismanagement"


def _preceding(task_names: list, host: str) -> list:
    lines = []
    n_drop = random.randint(0, max(0, len(task_names) - 1))
    tasks_used = task_names[n_drop:]
    for name in tasks_used:
        status = random.choice(["ok", "changed"])
        lines += [f"TASK [{name}] {STARS}", f"{status}: [{host}]", ""]
    return lines


def build_synthetic_log(iac_cat: str) -> tuple[str, str, str]:
    bp = random.choice(BLUEPRINTS[iac_cat])
    preceding_tasks, module, msg_template, fault_cat, fmt_pool = bp
    host      = random.choice(HOSTS)
    msg       = _fill(msg_template, host)
    preceding = _preceding(preceding_tasks, host)
    fmt       = random.choice(fmt_pool)
    fatal     = render_fatal(msg, module, host, fmt)
    return ("\n".join(preceding) + fatal).strip(), iac_cat, fault_cat


def build_real_log(error_type: str, raw_msg: str) -> tuple[str, str, str]:
    iac_cat, fault_cat = map_real_message(error_type)
    host = random.choice(HOSTS)
    n    = random.randint(1, 2)
    preceding_tasks = random.sample([t for t, _ in PRECEDING_OK_TASKS], n)
    preceding = _preceding(preceding_tasks, host)
    clean_msg = raw_msg.strip()
    if clean_msg.upper().startswith("ERROR!"):
        clean_msg = clean_msg[6:].strip()
    # Real messages always use failed_task or error_global_verbose
    fmt = random.choices(["failed_task", "error_global_verbose", "error_global_simple"],
                         weights=[0.45, 0.35, 0.20])[0]
    fatal    = render_fatal(clean_msg, None, host, fmt)
    log_text = ("\n".join(preceding) + fatal).strip()
    return log_text, iac_cat, fault_cat


def build_success_log() -> str:
    task, status = random.choice(PRECEDING_OK_TASKS)
    host  = random.choice(HOSTS)
    extra = random.choice(["", ' => {"changed": false}', ' => {"changed": true, "rc": 0}'])
    return f"TASK [{task}] {STARS}\n{status}: [{host}]{extra}"


def generate(
    n_synthetic: int = 15000,
    n_success:   int = 5000,
    out_anomaly: str = "ansible_anomaly_pool_ost",
    out_stream:  str = "ansible_full_stream_ost.csv",
    real_csv:    str = "Ansible_errors_raw.csv",
):
    real_df   = pd.read_csv(real_csv)
    real_rows = []
    for _, row in real_df.iterrows():
        log, iac, fault = build_real_log(str(row["Error Type"]), str(row["Error message"]))
        real_rows.append({"log": log, "iac_category": iac, "fault_category": fault})
    print(f"  Real messages loaded: {len(real_rows)}")

    # OST defect proportions from Table 9 (Rahman et al., Gang of Eight).
    # BLUEPRINTS dict key order:
    # Security, Service, Dependency, Configuration Data,
    # Idempotency, Conditional, Syntax, Documentation
    iac_cats = list(BLUEPRINTS.keys())
    ost_raw  = [0.5, 1.8, 2.4, 11.5, 0.3, 0.3, 2.3, 1.6]
    ost_sum  = sum(ost_raw)
    weights  = [w / ost_sum for w in ost_raw]
    per_cat  = {c: max(1, round(n_synthetic * w))
                for c, w in zip(iac_cats, weights)}
    # Fix rounding so synthetic total equals n_synthetic exactly
    _diff = n_synthetic - sum(per_cat.values())
    per_cat["Configuration Data"] += _diff

    synth_rows = []
    for iac_cat, count in per_cat.items():
        for _ in range(count):
            log, iac, fault = build_synthetic_log(iac_cat)
            synth_rows.append({"log": log, "iac_category": iac, "fault_category": fault})

    all_anomaly = synth_rows + real_rows
    random.shuffle(all_anomaly)
    df_anomaly = pd.DataFrame(all_anomaly)

    df_train, df_test = train_test_split(
        df_anomaly, test_size=5000, random_state=42,
        stratify=df_anomaly["iac_category"]
    )

    success_rows = [{"log": build_success_log(),
                     "iac_category": "SUCCESS", "fault_category": "NONE"}
                    for _ in range(n_success)]
    df_full = pd.concat([
        df_test.sample(n=500, random_state=42),
        pd.DataFrame(success_rows)
    ]).sample(frac=1, random_state=42).reset_index(drop=True)

    train_path = f"{out_anomaly}_train.csv"
    test_path  = f"{out_anomaly}_test.csv"
    df_train.to_csv(train_path, index=False)
    df_test.to_csv(test_path,   index=False)
    df_full.to_csv(out_stream,  index=False)

    print("\n" + "=" * 60)
    print("DATASET GENERATED (v3)")
    print("=" * 60)
    print(f"  Train pool : {len(df_train):>6} rows  →  {train_path}")
    print(f"  Test pool  : {len(df_test):>6} rows  →  {test_path}")
    print(f"  Full stream: {len(df_full):>6} rows  →  {out_stream}")
    print()
    print("IaC category distribution (test pool):")
    print(df_test["iac_category"].value_counts().to_string())
    print()
    print("Fault category distribution (test pool):")
    print(df_test["fault_category"].value_counts().to_string())


if __name__ == "__main__":
    generate()

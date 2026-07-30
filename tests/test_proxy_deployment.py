import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ProxyDeploymentContractTest(unittest.TestCase):
    def test_demo_proxy_caps_connections_and_targets_loopback_app(self):
        service = (
            PROJECT_ROOT
            / "deploy/systemd/cex-dex-dashboard-proxy.service.in"
        ).read_text(encoding="utf-8")
        socket = (
            PROJECT_ROOT
            / "deploy/systemd/cex-dex-dashboard-proxy.socket.in"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "systemd-socket-proxyd --connections-max=64 127.0.0.1:8766",
            service,
        )
        self.assertIn("NoNewPrivileges=true", service)
        self.assertIn("ListenStream=@PUBLIC_BIND_HOST@:8765", socket)
        self.assertIn("Backlog=128", socket)

    def test_demo_proxy_runbook_forbids_untrusted_forwarded_ip(self):
        runbook = (
            PROJECT_ROOT / "docs/production-hardening.md"
        ).read_text(encoding="utf-8")
        self.assertIn("Connection-capped demo proxy", runbook)
        self.assertIn("TRUST_LOOPBACK_PROXY_CLIENT_IP=false", runbook)
        self.assertIn(
            "Never set\n`TRUST_LOOPBACK_PROXY_CLIENT_IP=true`",
            runbook,
        )


if __name__ == "__main__":
    unittest.main()

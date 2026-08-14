const { defineConfig, devices } = require("playwright/test");

module.exports = defineConfig({
  testDir: "./tests",
  fullyParallel: false,
  // This constrained local browser/server runtime drops required assets under
  // concurrent workers; serialize so connection resets remain test failures.
  workers: 1,
  use: {
    baseURL: "http://127.0.0.1:8767",
    trace: "retain-on-failure",
  },
  webServer: {
    command: "cd .. && python3 dashboard/server.py --host 127.0.0.1 --port 8767",
    url: "http://127.0.0.1:8767/",
    reuseExistingServer: false,
    timeout: 30_000,
  },
  projects: [
    {
      name: "desktop",
      use: { ...devices["Desktop Chrome"], viewport: { width: 1440, height: 900 } },
    },
    {
      name: "mobile",
      use: {
        ...devices["Pixel 5"],
        browserName: "chromium",
        viewport: { width: 390, height: 844 },
      },
    },
  ],
});

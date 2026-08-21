module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  roots: ["<rootDir>/tests"],
  transform: {
    "^.+\\.tsx?$": "ts-jest",
  },
  // Integration tests share one real Postgres instance and each file
  // TRUNCATEs shared tables in beforeEach — running suites in parallel
  // workers causes one file's cleanup to wipe another's in-flight data.
  maxWorkers: 1,
};

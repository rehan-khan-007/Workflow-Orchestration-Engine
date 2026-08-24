import { validateDag } from "../../src/core/validation";
import { Step } from "../../src/types";

function step(id: string, dependsOn: string[] = []): Step {
  return { id, dependsOn, status: "pending" };
}

describe("validateDag", () => {
  it("accepts an empty step list", () => {
    expect(validateDag([]).valid).toBe(true);
  });

  it("accepts a single step with no dependencies", () => {
    expect(validateDag([step("a")]).valid).toBe(true);
  });

  it("accepts a valid linear chain", () => {
    const result = validateDag([step("a"), step("b", ["a"]), step("c", ["b"])]);
    expect(result.valid).toBe(true);
  });

  it("accepts a valid diamond (fan-out then fan-in)", () => {
    const result = validateDag([
      step("a"),
      step("b", ["a"]),
      step("c", ["a"]),
      step("d", ["b", "c"]),
    ]);
    expect(result.valid).toBe(true);
  });

  it("accepts a wide fan-out with no shared dependents", () => {
    const steps = Array.from({ length: 10 }, (_, i) => step(`s${i}`));
    expect(validateDag(steps).valid).toBe(true);
  });

  it("rejects a duplicate step id", () => {
    const result = validateDag([step("a"), step("a")]);
    expect(result.valid).toBe(false);
    expect(result.error).toMatch(/duplicate/i);
  });

  it("rejects a step depending on an unknown step", () => {
    const result = validateDag([step("a", ["ghost"])]);
    expect(result.valid).toBe(false);
    expect(result.error).toMatch(/unknown step/i);
  });

  it("rejects a step that depends on itself", () => {
    const result = validateDag([step("a", ["a"])]);
    expect(result.valid).toBe(false);
    expect(result.error).toMatch(/depends on itself/i);
  });

  it("rejects a direct two-step cycle", () => {
    const result = validateDag([step("a", ["b"]), step("b", ["a"])]);
    expect(result.valid).toBe(false);
    expect(result.error).toMatch(/cycle/i);
  });

  it("rejects a longer indirect cycle", () => {
    const result = validateDag([
      step("a", ["c"]),
      step("b", ["a"]),
      step("c", ["b"]),
    ]);
    expect(result.valid).toBe(false);
    expect(result.error).toMatch(/cycle/i);
  });

  it("accepts a DAG with an isolated step alongside a connected chain", () => {
    const result = validateDag([step("isolated"), step("a"), step("b", ["a"])]);
    expect(result.valid).toBe(true);
  });
});

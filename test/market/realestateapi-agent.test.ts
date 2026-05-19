import { describe, expect, it } from "vitest";
import { bestRealEstateApiAgent } from "../../src/market/realestateapi-agent";

describe("bestRealEstateApiAgent", () => {
  it("prefers an email-bearing MLS history row over an earlier name-only row", () => {
    const agent = bestRealEstateApiAgent({
      mlsHistory: [
        { agentName: "Earlier Name", agentPhone: "317-555-0100" },
        { agentName: "Email Agent", agentEmail: "email.agent@example.com", agentOffice: "Example Realty" },
      ],
    });

    expect(agent).toEqual({
      name: "Email Agent",
      email: "email.agent@example.com",
      phone: null,
      brokerage: "Example Realty",
    });
  });

  it("falls back to phone/name contact data when no email exists", () => {
    const agent = bestRealEstateApiAgent({
      mlsHistory: [
        { agentName: "Name Only" },
        { agentName: "Phone Agent", agentPhone: "317-555-0101" },
      ],
    });

    expect(agent).toMatchObject({
      name: "Phone Agent",
      email: null,
      phone: "317-555-0101",
    });
  });
});

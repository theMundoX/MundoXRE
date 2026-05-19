import { describe, expect, it } from "vitest";

import {
  brokerageDomainHints,
  emailLocalMatchesName,
  searchBrokerageText,
  verifyEmailPage,
} from "../../scripts/enrich-agent-emails-public";

const baseRow = {
  id: 1,
  address: "7404 FRANKLIN PARKE BLVD",
  city: "INDIANAPOLIS",
  state_code: "IN",
  listing_agent_first_name: null,
  listing_agent_last_name: null,
  listing_agent_phone: "3178625200",
  listing_brokerage: "Hoosier, REALTORS®",
  listing_url: "https://www.redfin.com/IN/Indianapolis/example",
  listing_source: "redfin",
  raw: null,
};

describe("public agent email matching", () => {
  it("normalizes noisy brokerage text for search", () => {
    expect(searchBrokerageText("Hoosier, REALTORS®")).toBe("Hoosier, realtors");
    expect(searchBrokerageText("Mark Dietel Realty, LLC")).toBe("Mark Dietel Realty");
  });

  it("adds known Indianapolis brokerage domains", () => {
    expect(brokerageDomainHints("Hoosier, REALTORS®")).toContain("hoosier-realtors.com");
    expect(brokerageDomainHints("Mark Dietel Realty, LLC")).toContain("markdietel.com");
  });

  it("accepts conservative common nicknames in email local parts", () => {
    expect(emailLocalMatchesName("jim@hoosier-realtors.com", { first: "James", last: "Talhelm" })).toBe(true);
    expect(emailLocalMatchesName("mike@example.com", { first: "Michael", last: "Smith" })).toBe(true);
    expect(emailLocalMatchesName("kim@hoosier-realtors.com", { first: "Kimberly", last: "Lyon" })).toBe(true);
    expect(emailLocalMatchesName("njnichol@gmail.com", { first: "Nathan", last: "Nicholson" })).toBe(true);
  });

  it("verifies broker-domain nickname email when phone and brokerage match", () => {
    const candidate = verifyEmailPage(
      `
      <html>
        <body>
          Hoosier, REALTORS. Call 317-862-5200.
          <a href="mailto:jim@hoosier-realtors.com">jim@hoosier-realtors.com</a>
        </body>
      </html>
      `,
      { ...baseRow, listing_agent_name: "James Talhelm" },
      "https://hoosier-realtors.com/jim-talhem",
    );

    expect(candidate?.email).toBe("jim@hoosier-realtors.com");
    expect(candidate?.confidence).toBe("public_profile_verified");
  });

  it("verifies alias email in the same agent result block", () => {
    const candidate = verifyEmailPage(
      `
      <html>
        <body>
          Listing By: Milissa Shupert - Mark Dietel Realty, LLC
          mashupert@gmail.com
          Phone 317-333-3726.
        </body>
      </html>
      `,
      {
        ...baseRow,
        id: 2,
        listing_agent_name: "Milissa Shupert",
        listing_agent_phone: "3173333726",
        listing_brokerage: "Mark Dietel Realty, LLC",
      },
      "https://example.test/listing",
    );

    expect(candidate?.email).toBe("mashupert@gmail.com");
    expect(candidate?.confidence).toBe("public_profile_name_email_proximity");
  });

  it("rejects another person's nearby email on a noisy result page", () => {
    const candidate = verifyEmailPage(
      `
      <html>
        <body>
          Listing agent Ann Krider. Contact nearby office partner David Jr at david.jr@twifordfh.com.
          Phone 317-417-4554. Real estate listing details.
        </body>
      </html>
      `,
      {
        ...baseRow,
        id: 3,
        listing_agent_name: "Ann Krider",
        listing_agent_phone: "3174174554",
        listing_brokerage: "Ann Krider",
      },
      "https://example.test/noisy-result",
    );

    expect(candidate).toBeNull();
  });
});

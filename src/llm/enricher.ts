/**
 * Property Enrichment using LLM
 * Analyzes raw property data and adds insights
 */

import { getOrCreateRouter } from "./provider.js";
import type { Property } from "../db/queries.js";

const router = getOrCreateRouter();

export interface EnrichmentResult {
  propertyType: string;
  investmentGrade: "A" | "B" | "C" | "D";
  riskFactors: string[];
  summary: string;
  provider: "claude" | "mundox";
}

/**
 * Enrich a property with AI analysis
 */
export async function enrichProperty(property: Property): Promise<EnrichmentResult | null> {
  try {
    const prompt = `Analyze this property record and provide investment insights:

Address: ${property.address}
City: ${property.city}, ${property.state_code}
Type: ${property.property_type}
Bedrooms: ${property.bedrooms}
Bathrooms: ${property.bathrooms}
Assessed Value: $${property.assessed_value}
Annual Tax: $${property.annual_tax}
Year Built: ${property.year_built}
Owner: ${property.owner_name}
${property.lien_status ? `Lien Status: ${property.lien_status}` : ""}

Provide a JSON response with:
{
  "propertyType": "single_family|multifamily|commercial|other",
  "investmentGrade": "A|B|C|D",
  "riskFactors": ["list", "of", "risks"],
  "summary": "1-2 sentence investment thesis"
}`;

    const response = await router.call({
      prompt,
      maxTokens: 300,
      system: "You are a real estate investment analyst. Provide JSON responses only, no markdown.",
    });

    // Parse JSON response
    const jsonMatch = response.text.match(/\{[\s\S]*\}/);
    if (!jsonMatch) {
      console.warn(`Failed to parse LLM response for ${property.address}`);
      return null;
    }

    const analysis = JSON.parse(jsonMatch[0]);
    return {
      propertyType: analysis.propertyType || property.property_type,
      investmentGrade: analysis.investmentGrade || "C",
      riskFactors: analysis.riskFactors || [],
      summary: analysis.summary || "",
      provider: response.provider,
    };
  } catch (err) {
    console.error(`Enrichment error for ${property.address}:`, err instanceof Error ? err.message : err);
    return null;
  }
}

/**
 * Batch enrich properties
 */
export async function enrichBatch(properties: Property[]): Promise<Map<number, EnrichmentResult>> {
  const results = new Map<number, EnrichmentResult>();

  for (const prop of properties) {
    const enriched = await enrichProperty(prop);
    if (enriched && prop.id) {
      results.set(prop.id, enriched);
    }
  }

  return results;
}

/**
 * Normalize/clean raw scraped data using LLM
 */
export async function normalizeWithLLM(rawData: Record<string, any>): Promise<Record<string, any>> {
  try {
    const prompt = `Normalize this scraped property record into standard format. Return JSON only:

Raw Data: ${JSON.stringify(rawData)}

Output a JSON object with standardized keys:
{
  "address": "street address",
  "city": "city name",
  "state": "2-letter state code",
  "zip": "zip code",
  "parcel_id": "unique parcel identifier",
  "property_type": "single_family|multifamily|commercial|land|other",
  "bedrooms": number or null,
  "bathrooms": number or null,
  "sqft": number or null,
  "year_built": number or null,
  "owner_name": "owner name",
  "assessed_value": number or null,
  "annual_tax": number or null,
  "notes": "any data quality issues"
}`;

    const response = await router.call({
      prompt,
      maxTokens: 400,
      system: "You are a real estate data normalization expert. Output valid JSON only.",
    });

    const jsonMatch = response.text.match(/\{[\s\S]*\}/);
    if (!jsonMatch) {
      return rawData; // Return raw if parsing fails
    }

    return JSON.parse(jsonMatch[0]);
  } catch (err) {
    console.warn("LLM normalization failed, returning raw data");
    return rawData;
  }
}

#!/usr/bin/env tsx
import { estimateRent } from "../src/utils/rent-estimator.js";

// 5444 Gaston Ave, Dallas TX — 11 units, 8,416 sqft total
const totalUnits = 11;
const totalSqft = 8416;
const assessedValue = 1290000;

const perUnitSqft = Math.round(totalSqft / totalUnits);
const perUnitValue = Math.round(assessedValue / totalUnits);

console.log("5444 GASTON AVE, DALLAS TX 75214");
console.log(`Total: ${totalUnits} units, ${totalSqft.toLocaleString()} sqft, $${assessedValue.toLocaleString()} assessed`);
console.log(`Per unit: ${perUnitSqft} sqft, $${perUnitValue.toLocaleString()} value\n`);

const estimate = estimateRent({
  city: "DALLAS",
  sqft: perUnitSqft,
  yearBuilt: 1961,
  assessedValue: perUnitValue,
  totalUnits,
});

console.log("Rent Estimate (per unit):");
console.log(`  Beds:         ${estimate.beds}BR`);
console.log(`  Monthly Rent: $${estimate.estimated_rent}`);
console.log(`  $/Sq Ft:      $${estimate.estimated_rent_psf}`);
console.log(`  Confidence:   ${estimate.confidence_level} (${estimate.confidence_score})`);
console.log(`  Method:       ${estimate.estimation_source}`);
console.log(`  FMR Compare:  $${estimate.fmr_rent}`);

const grossMonthly = estimate.estimated_rent * totalUnits;
const grossAnnual = grossMonthly * 12;
console.log(`\nBuilding-Level Metrics:`);
console.log(`  Gross Monthly:  $${grossMonthly.toLocaleString()}`);
console.log(`  Gross Annual:   $${grossAnnual.toLocaleString()}`);
console.log(`  GRM:            ${(assessedValue / grossAnnual).toFixed(1)}`);
console.log(`  Cap Rate:       ${((grossAnnual / assessedValue) * 100).toFixed(1)}%`);

#!/usr/bin/env tsx
/**
 * Backward-compatible wrapper.
 *
 * The dashboard is now one tabbed multi-market file. Existing Dallas refresh
 * jobs can keep calling this script and will regenerate the unified dashboard.
 */
import "./generate-market-coverage-dashboard.ts";

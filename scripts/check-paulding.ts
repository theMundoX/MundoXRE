#!/usr/bin/env tsx
import * as shapefile from "shapefile";
const source = await shapefile.open("data/paulding-oh-parcels/Parcels.shp");
const first = await source.read();
console.log("Fields:", Object.keys(first.value.properties || {}).join(", "));
console.log("Sample:", JSON.stringify(first.value.properties).slice(0, 500));

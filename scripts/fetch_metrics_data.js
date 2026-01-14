const fs = require("fs");
const path = require("path");
const env = (typeof process !== "undefined" && process.env) ? process.env : {};
const emails = JSON.parse(env.EMAILS_JSON || "[]");
if (!emails.length) {
  print("No emails provided.");
  quit(0);
}

const dbName = env.DB_NAME || "metricsdb";
const collName = env.COLL_NAME || "metrics";
const analysisDir = env.ANALYSIS_DIR || "analysis";
const allowDiskUse = (env.ALLOW_DISK_USE || "").toLowerCase() === "true";

const coll = db.getSiblingDB(dbName).getCollection(collName);
const aggOpts = { allowDiskUse };

if (!fs.existsSync(analysisDir)) {
  fs.mkdirSync(analysisDir, { recursive: true });
}

// Partner-specific hints for RI field and display info
const partners = [
  { match: /iglesias/i, name: "Jaime Iglesias", jobType: "detail_cloud", riField: "SiteName" },
  { match: /kostas/i, name: "Kostas Chounos", jobType: "detail_network", riField: "Site" },
  { match: /atsareg/i, name: "Andrei Tsaregorodtsev / Mazen Ezzeddine", jobType: "detail_grid", riField: "SiteGOCDB" }
];
const tableRows = [];

emails.forEach((email) => {
  // Action: count total submissions
  const total = coll.countDocuments({ publisher_email: email });
  if (!total) {
    print("Processed " + email + ": total=0 (skipped export)");
    return;
  }
  print("Processed " + email + ": total=" + total);
  // Action: monthly submission counts
  const perMonth = coll.aggregate(
    [
      { $match: { publisher_email: email } },
      { $addFields: { ts: { $toDate: "$timestamp" } } },
      {
        $group: {
          _id: { year: { $year: "$ts" }, month: { $month: "$ts" } },
          submissions: { $sum: 1 }
        }
      },
      { $sort: { "_id.year": 1, "_id.month": 1 } },
      { $project: { _id: 0, year: "$_id.year", month: "$_id.month", submissions: 1 } }
    ],
    aggOpts
  ).toArray();
  // Action: latest example doc
  const latestDoc = coll.find({ publisher_email: email }).sort({ timestamp: -1 }).limit(1).toArray()[0] || null;
  if (latestDoc && latestDoc._id && typeof latestDoc._id.toString === "function") {
    latestDoc._id = latestDoc._id.toString();
  }
  if (latestDoc && latestDoc.timestamp instanceof Date) {
    latestDoc.timestamp = latestDoc.timestamp.toISOString();
  }

  // Action: total rows (arrays count their items; numeric body.rows respected; default 1 per doc)
  const rowsAgg = coll.aggregate(
    [
      { $match: { publisher_email: email } },
      {
        $group: {
          _id: null,
          rowsTotal: {
            $sum: {
              $let: {
                vars: { rowsField: "$body.rows" },
                in: {
                  $cond: [
                    { $eq: [{ $type: "$body" }, "array"] },
                    { $size: "$body" },
                    {
                      $cond: [
                        { $in: [{ $type: "$$rowsField" }, ["int", "long", "double", "decimal"]] },
                        "$$rowsField",
                        1
                      ]
                    }
                  ]
                }
              }
            }
          }
        }
      }
    ],
    aggOpts
  ).toArray()[0];
  const totalRows = rowsAgg ? rowsAgg.rowsTotal : 0;

  // Action: job type and status breakdowns
  const jobTypeCounts = coll.aggregate(
    [
      { $match: { publisher_email: email } },
      { $group: { _id: "$body.JobType", count: { $sum: 1 } } },
      { $project: { _id: 0, jobType: "$_id", count: 1 } },
      { $sort: { count: -1, jobType: 1 } }
    ],
    aggOpts
  ).toArray();
  const statusCounts = coll.aggregate(
    [
      { $match: { publisher_email: email } },
      { $group: { _id: "$body.Status", count: { $sum: 1 } } },
      { $project: { _id: 0, status: "$_id", count: 1 } },
      { $sort: { count: -1, status: 1 } }
    ],
    aggOpts
  ).toArray();

  // Action: RI hints from payload fields
  const riHints = coll.aggregate(
    [
      { $match: { publisher_email: email } },
      {
        $project: {
          ri: [
            "$body.SiteGOCDB",
            "$body.SiteDIRAC",
            "$body.Site",
            "$body.Owner",
            "$body.OwnerGroup",
            "$body.OwnerDN"
          ]
        }
      },
      { $unwind: "$ri" },
      { $match: { ri: { $ne: null, $ne: "" } } },
      { $group: { _id: "$ri", count: { $sum: 1 } } },
      { $project: { _id: 0, value: "$_id", count: 1 } },
      { $sort: { count: -1, value: 1 } }
    ],
    aggOpts
  ).toArray();

  // Action: status flag (pushing <=14d, stopped <=30d, else inactive)
  let status = "inactive";
  if (latestDoc && latestDoc.timestamp) {
    const lastTs = new Date(latestDoc.timestamp);
    if (!isNaN(lastTs)) {
      const ageDays = (Date.now() - lastTs.getTime()) / 86400000;
      if (ageDays <= 14) {
        status = "pushing";
      } else if (ageDays <= 30) {
        status = "stopped";
      }
    }
  }

  const out = {
    email,
    total,
    totalRows,
    perMonth,
    latest: latestDoc,
    jobTypeCounts,
    statusCounts,
    status,
    riHints
  };
  const slug = email.replace(/[^a-z0-9]+/gi, "_") || "metrics";
  const outPath = path.join(analysisDir, `${slug}.json`);
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  print("  -> saved " + outPath);

  // Action: build summary row for CSV export
  const partner = partners.find((p) => p.match.test(email)) || null;
  let riValue = null;
  let riFieldUsed = null;
  if (partner && partner.riField) {
    const riAgg = coll.aggregate(
      [
        { $match: { publisher_email: email } },
        { $group: { _id: `$body.${partner.riField}`, count: { $sum: 1 } } },
        { $match: { _id: { $ne: null, $ne: "" } } },
        { $sort: { count: -1 } },
        { $limit: 1 },
        { $project: { _id: 0, value: "$_id" } }
      ],
      aggOpts
    ).toArray()[0];
    if (riAgg) {
      riValue = riAgg.value;
      riFieldUsed = partner.riField;
    }
  }
  const topJobType = jobTypeCounts.length ? jobTypeCounts[0].jobType : "";
  const latestTimestamp = latestDoc && latestDoc.timestamp ? latestDoc.timestamp : "";

  tableRows.push({
    email,
    name: partner ? partner.name : "",
    mapped_job_type: partner ? partner.jobType : "",
    top_job_type: topJobType || "",
    total,
    total_rows: totalRows,
    status: status || "",
    latest_timestamp: latestTimestamp,
    ri_field: riFieldUsed || "",
    ri_value: riValue || ""
  });
});

// Action: write CSV summary
if (tableRows.length) {
  const headers = [
    "email",
    "name",
    "mapped_job_type",
    "top_job_type",
    "total",
    "total_rows",
    "status",
    "latest_timestamp",
    "ri_field",
    "ri_value"
  ];
  const esc = (val) => {
    const s = (val === undefined || val === null) ? "" : String(val);
    if (s.includes('"') || s.includes(",") || s.includes("\n")) {
      return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
  };
  const lines = [headers.join(",")];
  tableRows.forEach((row) => {
    lines.push(headers.map((h) => esc(row[h])).join(","));
  });
  const csvPath = path.join(analysisDir, "summary.csv");
  fs.writeFileSync(csvPath, lines.join("\n"));
  print("Summary CSV saved to " + csvPath);
}

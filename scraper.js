const sqlite3 = require("sqlite3");
const axios = require("axios");

const BCC_DEVELOPMENT_APPLICATION_ENDPOINT = "https://developmenti.brisbane.qld.gov.au/Geo/GetApplicationFilterResults";
const BCC_INFORMATION_URL = "https://developmenti.brisbane.qld.gov.au/Home/FilterDirect?filters=DANumber=";

const LIMIT_PER_REQUEST = 200;

// Sets up an sqlite database.
// Credit to https://github.com/planningalerts-scrapers/regional_council_of_goyder_sa_development_applications
async function initializeDatabase() {
  return new Promise((resolve, reject) => {
    const database = new sqlite3.Database("data.sqlite");
    database.serialize(() => {
      database.run("create table if not exists [data] ([council_reference] text primary key, [address] text, [description] text, [info_url] text, [date_scraped] text, [date_received] text)");
      resolve(database);
    });
  });
}

// Inserts a row in the database if the row does not already exist.
// Credit to https://github.com/planningalerts-scrapers/regional_council_of_goyder_sa_development_applications
async function insertRow(database, developmentApplication) {
  return new Promise((resolve, reject) => {
    const sqlStatement = database.prepare("insert or replace into [data] values (?, ?, ?, ?, ?, ?)");
    sqlStatement.run([
      developmentApplication.applicationNumber,
      developmentApplication.address,
      developmentApplication.description,
      developmentApplication.informationUrl,
      developmentApplication.scrapeDate,
      developmentApplication.receivedDate,
    ], function (error, row) {
      if (error) {
        console.error(error);
        reject(error);
      }
      else {
        console.log(`    Saved application \"${developmentApplication.applicationNumber}\" with address \"${developmentApplication.address}\", description \"${developmentApplication.description}\" and received date \"${developmentApplication.receivedDate}\" to the database.`);
        sqlStatement.finalize(); // releases any locks
        resolve(row);
      }
    });
  });
}

async function loadData(page=0) {

  const now = new Date();
  const lastTwoWeeks = new Date()
  lastTwoWeeks.setDate(lastTwoWeeks.getDate() - 14);

  const requestPayload = {
    "Progress": "all",
    "StartDateUnixEpochNumber": lastTwoWeeks.getTime(),
    "EndDateUnixEpochNumber": now.getTime(),
    "DateRangeField": "submitted",
    "DateRangeDescriptor": "Last 14 days",
    "LotPlan": null,
    "LandNumber": null,
    "PropNumber": null,
    "DANumber": null,
    "BANumber": null,
    "PlumbNumber": null,
    "IncludeDA": true,
    "IncludeBA": false,
    "IncludePlumb": false,
    "LocalityId": null,
    "DivisionId": null,
    "ApplicationTypeId": null,
    "SubCategoryUseId": null,
    "AssessmentLevels": [],
    "ShowCode": true,
    "ShowImpact": true,
    "ShowIAGA": true,
    "ShowIAGI": true,
    "ShowNotifiableCode": true,
    "ShowReferralResponse": true,
    "ShowRequest": true,
    "PagingStartIndex": page*LIMIT_PER_REQUEST,
    "MaxRecords": LIMIT_PER_REQUEST,
    "Boundary": null,
    "ViewPort": {
      "BoundaryType": "POLYGON",
      "GeometryPropertyName": "geom_point",
      "Boundary": [[{"Lat":-27.925913196829814,"Lng":152.2546140028},{"Lat":-27.925913196829814,"Lng":153.79558599720002},{"Lat":-27.011590411641816,"Lng":153.79558599720002},{"Lat":-27.011590411641816,"Lng":152.2546140028},{"Lat":-27.925913196829814,"Lng":152.2546140028}]]
    },
    "IncludeAroundMe": true,
    "SortField": "submitted",
    "SortAscending": true,
    "BBox": null,
    "PixelWidth": 800,
    "PixelHeight": 800
  }
  return axios.post(BCC_DEVELOPMENT_APPLICATION_ENDPOINT, requestPayload).then(response => {
    const applications = response.data;
    const multiSpotApplications = Object.values((applications.multiSpot || {'empty': []})).flat();
    return { totalFeatures: applications.totalFeatures, data: [...applications.features, ...multiSpotApplications] };
  });
}

async function loadAllData() {
  const { data: firstRequest, totalFeatures } = await loadData();
  let combinedOthers = [];
  if (totalFeatures > LIMIT_PER_REQUEST) {
    const iterations = Math.ceil(totalFeatures/LIMIT_PER_REQUEST)-1;
    const otherRequestsPromises = [];
    for(let i=1; i<=iterations; i++) {
      otherRequestsPromises.push(loadData(i));
    }
    const otherRequests = await Promise.all(otherRequestsPromises);
    combinedOthers = otherRequests.map(otherRequestResponse => otherRequestResponse.data).flat();
  }
  return [...firstRequest, ...combinedOthers];
}

function parseAddressDesc(desc) {
  const extracted = desc.split("-");
  const address = extracted[0].trim();
  extracted.shift();
  return {
    address,
    description: extracted.join("-").trim(),
  }
}

function formatCurrentDate(date=null) {
  var d = new Date(date),
    month = '' + (d.getMonth() + 1),
    day = '' + d.getDate(),
    year = d.getFullYear();

  if (month.length < 2)
    month = '0' + month;
  if (day.length < 2)
    day = '0' + day;

  return [year, month, day].join('-');
}

function deduplicate(arr) {
  const hashTable = {};
  return arr.filter(function (el) {
    const key = el.properties ? el.properties.application_number : "";
    return Boolean(hashTable[key]) ? false : hashTable[key] = true;
  });
}

async function main() {

  // Ensure that the database exists.
  const database = await initializeDatabase();
  // Not sure why, but API returns multiple entries with almost identical information for the same application just different land_no, UI seems to show only one as well, so we are simply dedupe them there
  const allApplications = deduplicate(await loadAllData() || []);
  const promises = allApplications.map(async application => {
    const details = application.properties || {};
    const { address = "", description = ""  } = parseAddressDesc(details.description);
    const applicationNumber = details.application_number;
    const developmentApplication = {
      applicationNumber: applicationNumber,
      address,
      description,
      informationUrl: `${BCC_INFORMATION_URL}${applicationNumber}`,
      scrapeDate: formatCurrentDate(),
      receivedDate: details.date_received,
    };
    return insertRow(database, developmentApplication);
  });

  await Promise.all(promises);
  console.log("Finish writing data");
}

main().then(() => console.log("Complete.")).catch(error => console.error(error));

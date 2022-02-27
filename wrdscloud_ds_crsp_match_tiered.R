# preliminaries ----------------------------------------------------------------
# Author: Chris Webster
# Script purpose/description: match dealscan and crsp all in one step
library(data.table)
library(tidyverse)
library(fst)
library(feather)
library(zoo)
library(RPostgres)
library(DBI)
library(usmap)
library(maps)
library(dbplyr)
library(fedmatch)
setwd("~/ds_matching/")

# FACILITY DATA ----------------------------------------------------------------
# leftover variables that aren't in WRDS
facility_tbl <- tbl(wrds, in_schema("dealscan", "facility"))
ds_facility <- facility_tbl %>%
  select(
    FacilityID = facilityid,
    CurrencyFacility = currency,
    PackageID = packageid,
    DistributionMethod = distributionmethod,
    ExchangeRateFacility = exchangerate,
    ConversionDate = conversiondate,
    AverageLife = averagelife,
    FacilityStartDate = facilitystartdate,
    FacilityEndDate = facilityenddate,
    FacilityAmt = facilityamt,
    LoanType = loantype,
    Maturity = maturity,
    PrimaryPurpose = primarypurpose,
    SecondaryPurpose = secondarypurpose,
    Renewal = renewal,
    Secured = secured,
    Seniority = seniority,
    TargetCompany = targetcompany
  ) %>%
  # head(10) %>%
  collect() %>%
  setDT()

# PACKAGE DATA -----------------------------------------------------------------

package_tbl <- tbl(wrds, in_schema("dealscan", "package"))
ds_package <- package_tbl %>%
  select(
    PackageID = packageid,
    Active = active,
    AssignmentFee = assignmentfee,
    AssignmentMin = assignmentmin,
    AssignmentRestrictions = assignmentrestrictions,
    CollateralRelease = collateralrelease,
    AgentConsent = agentconsent,
    CompanyConsent = companyconsent,
    CurrencyPackage = currency,
    DealPurpose = dealpurpose,
    DealAmount = dealamount,
    DealStatus = dealstatus,
    DefaultBaseRate = defaultbaserate,
    SpreadOverDefaultBase = spreadoverdefaultbase,
    DividendRestrictions = dividendrestrictions,
    ExcessCFSweep = excesscfsweep,
    ExchangeRatePackage = exchangerate,
    Hybrid = hybrid, PctofNetIncome = percentageofnetincome,
    CompanyID = borrowercompanyid,
    ProjectFinanceType = projectfinancetype,
    ProRataAllocation = prorataallocation,
    RefinancingIndicator = refinancingindicator,
    RequiredLenders = requiredlenders,
    SalesAtClose = salesatclose,
    AssetSalesSweep = assetsalessweep,
    DebtIssuanceSweep = debtissuancesweep,
    EquityIssuanceSweep = equityissuancesweep,
    ExcessCFSweep = excesscfsweep,
    InsuranceProceedsSweep = insuranceproceedssweep,
    TermChanges = termchanges
  ) %>%
  collect() %>%
  setDT()
# COMPANY DATA -----------------------------------------------------------------
# variables missing from query


company_tbl <- tbl(wrds, in_schema("dealscan", "company"))
ds_company <- company_tbl %>%
  select(
    CompanyID = companyid,
    City = city,
    Company = company,
    ParentID = parentid,
    InstitutionType = institutiontype,
    UltimateParentID = ultimateparentid,
    Country = country,
    PublicPrivate = publicprivate,
    Region = region,
    Sales = sales,
    PrimarySICCode = primarysiccode,
    SecondarySICCode = secondarysiccode,
    TertiarySICCode = tertiarysiccode,
    State = state,
    Ticker = ticker,
    ZipCode = zipcode
  ) %>%
  collect() %>%
  setDT()
# MERGE TOGETHER ---------------------------------------------------------------
facility_package <- merge(ds_facility, ds_package, by = c("PackageID"), all.x = T)
ds_raw <- merge(facility_package, ds_company, by = "CompanyID")

ds_initial_size <- nrow(ds_raw)

# keep all US Borrowers
ds_raw <- ds_raw[which(Country == "USA" & !is.na(Country)), ]
ds_initial_us <- nrow(ds_raw)

# remove old data records
ds_raw[, year := as.numeric(format(FacilityStartDate, "%Y"))]
ds_raw <- ds_raw[which(year > 1980 & !is.na(year)), ]

## Lower case cities
ds_raw[, City := tolower(City)]

# convert state names to abbreviations dealscan
ds_raw[, STATE := ifelse(State == "Washington, D.C.", "Washington D.C.", State)]
ds_raw[, HSTATE := fips(State)]
common_abbr_db <- read.csv("data/common_abbr.csv", stringsAsFactors = F)
sp_char_db <- data.frame(cbind(character = c("\\s*\\[[^\\)]+\\]", "\\&", "\\$", "\\%", "\\@"), replacement = c("", "and", "dollar", "percent", "at")), stringsAsFactors = F)
ds_raw[, cleanname := clean_strings(Company, sp_char_words = sp_char_db, common_words = common_abbr_db, remove_char = c("/", "-", "'"))]

## check duplicates
any(duplicated(ds_raw$FacilityID))

ds_final_size <- dim(ds_raw)[1] # final ds size
ds_pct_keep <- 100 * ds_final_size / ds_initial_size
ds_pct_keep_us <- 100 * ds_final_size / ds_initial_us


#--------------------------------------
# CRSP COMPHIST data (post-April 2007)
#--------------------------------------

q <- dbSendQuery(wrds, "SELECT CAST(gvkey as varchar(6)) as gvkey, 
                              CAST(hchgdt AS DATE) as hchgdt, 
                              CAST(hchgenddt AS DATE) as hchgenddt, 
                              hconm, hconml, hein,  HSIC, HNAICS, HCITY, HSTATE, HADDZIP, HFIC, HINCORP
                       FROM crsp_q_ccm.comphist")

comphist <- dbFetch(q)
dbClearResult(q)

setDT(comphist)
comphist[, (c("hchgdt", "hchgenddt")) := lapply(.SD, function(x) fifelse(is.na(x), as.Date("9999-12-31"), x)), .SDcols = c("hchgdt", "hchgenddt")]
comphist[, crsp_table := "comphist"]


#-------------------------------------
# CRSP CST_HIST data (pre-April 2007)
#-------------------------------------
q <- dbSendQuery(wrds, "SELECT CAST(gvkey as int) as gvkey, 
                                     CAST(chgdt as varchar(8)) as hchgdt, 
                                     CAST(chgenddt as varchar(8)) as hchgenddt, 
                                     coname as hconm, EIN as hEIN, 
                                     CAST(dnum as varchar(4)) as HSIC, 
                                     CAST(naics as varchar(6)) as hnaics, 
                                     state as FIPS, cnum, FINC, stinc, smbl, zlist 
                       FROM crsp_q_ccm.cst_hist")
cst_hist <- dbFetch(q)
dbClearResult(q)
setDT(cst_hist)

cst_hist[, gvkey := as.character(sprintf("%06.0f", gvkey))]
cst_hist[, (c("hchgdt", "hchgenddt")) := lapply(.SD, function(x) fifelse(is.na(x) | x == "99999999", "20070413", x)), .SDcols = c("hchgdt", "hchgenddt")]
cst_hist[, (c("hchgdt", "hchgenddt")) := lapply(.SD, function(x) as.Date(x, format = "%Y%m%d")), .SDcols = c("hchgdt", "hchgenddt")]
cst_hist[, crsp_table := "cst_hist"]
cst_hist[, hnaics := trimws(hnaics)]
cst_hist[, (c("fips", "stinc")) := lapply(.SD, function(x) as.character(sprintf("%02.0f", x))), .SDcols = c("fips", "stinc")]

# merge in the FIPS codes for hstate;
fips <- state.fips[, c("fips", "abb")]
fips <- unique(fips)
setDT(fips)
setnames(fips, old = "abb", "hstate")
fips[, fips := as.character(sprintf("%02.0f", fips))]
cst_hist <- merge(cst_hist, fips, by = "fips", all.x = T)

# merge in the FIPS codes for HINCORP;
setnames(fips, old = c("fips", "hstate"), new = c("stinc", "hincorp"))
cst_hist <- merge(cst_hist, fips, by = "stinc", all.x = T)

## merge in the country codes;
cs_country_codes <- fread("data/CS_country_codes.csv", sep = ",", stringsAsFactors = F)
cs_country_codes <- cs_country_codes[, list(cntry_numeric_cd, cntry_alpha_cd)]
setnames(cs_country_codes, old = c("cntry_numeric_cd", "cntry_alpha_cd"), new = c("finc", "hfic"))
cst_hist <- merge(cst_hist, cs_country_codes, by = "finc", all.x = T)

## cleanup
cst_hist[, c("stinc", "fips", "finc") := NULL]

#---------------------
# stack the datasets
#---------------------
crsp_hist <- rbindlist(list(cst_hist, comphist), use.names = T, fill = T)

#-------------------------------------------------------------------
# create unique intervals for each gvkey, EIN, and name combination
#-------------------------------------------------------------------
crsp_hist <- crsp_hist[, list(
  d_dt_start = min(hchgdt),
  crsp_dt_end = max(hchgenddt)
),
by = list(
  gvkey, hconm, hconml,
  smbl, zlist, hein, cnum, hnaics, hsic, hfic,
  hincorp, hstate, hcity, haddzip, crsp_table
)
]

#---------------------------
# fix overlapping intervals
#---------------------------

## find overlapping intervals
crsp_hist <- crsp_hist[order(gvkey, -d_dt_start), ]
crsp_hist[, ldt_start := shift(d_dt_start, 1), by = list(gvkey)]
crsp_hist[, diff_dt_start := ifelse(is.na(crsp_dt_end) | crsp_dt_end == as.Date("9999-12-31"), NA, crsp_dt_end - ldt_start)]

## correct the overlaps
crsp_hist[, d_dt_end := fifelse(diff_dt_start > 0 & !is.na(diff_dt_start), ldt_start - 1, crsp_dt_end)]

## clean up
crsp_hist[, c("ldt_start", "diff_dt_start") := NULL]

#----------------
# Backfill items
#----------------

## copy items to backfill;
char_cols <- c("hein_fill", "hstate_fill", "hfic_fill", "zlist_fill")
crsp_hist[, (char_cols) := .SD, .SDcols = c("hein", "hstate", "hfic", "zlist")]

# back fill;
crsp_hist <- crsp_hist[order(gvkey, -d_dt_start), ]
crsp_hist[, (char_cols) := lapply(.SD, factor), .SDcols = char_cols]
lev <- sapply(char_cols, function(x) levels(crsp_hist[[x]]))
crsp_hist[, (char_cols) := lapply(.SD, as.integer), .SDcols = char_cols]
crsp_hist[, (char_cols) := lapply(.SD, nafill, "locf"), by = "gvkey", .SDcols = char_cols]
for (col in char_cols) set(crsp_hist, NULL, col, lev[[col]][crsp_hist[[col]]])

#--------------------
# Forward fill items
#--------------------

## copy items to forward fill;
char_cols <- c("hein_fill", "hstate_fill", "hfic_fill", "zlist_fill")

# forward fill;
crsp_hist <- crsp_hist[order(gvkey, d_dt_start), ]
crsp_hist[, (char_cols) := lapply(.SD, factor), .SDcols = char_cols]
lev <- sapply(char_cols, function(x) levels(crsp_hist[[x]]))
crsp_hist[, (char_cols) := lapply(.SD, as.integer), .SDcols = char_cols]
crsp_hist[, (char_cols) := lapply(.SD, nafill, "locf"), by = "gvkey", .SDcols = char_cols]
for (col in char_cols) set(crsp_hist, NULL, col, lev[[col]][crsp_hist[[col]]])

#---------------
# Final cleanup
#---------------
crsp_hist <- crsp_hist[, list(
  gvkey, d_dt_start, d_dt_end, crsp_dt_end,
  hconml, hconm, smbl, zlist, hein, cnum, hnaics, hsic,
  hfic, hincorp, hstate, hcity, haddzip, crsp_table
)]
crsp_hist <- crsp_hist[order(gvkey, d_dt_start, d_dt_end), ]
crsp <- copy(crsp_hist)
## create an id
crsp[, crsp_id := .GRP, by = list(gvkey, d_dt_start)]
any(duplicated(crsp$crsp_id))
# crsp
# set the name
crsp[, name := ifelse(is.na(hconml), hconm, hconml)]

## convert states to fips
crsp[, STATE := fips(hstate)]

## clean company names

common_abbr_db <- read.csv("data/common_abbr.csv", stringsAsFactors = F)
sp_char_db <- data.frame(cbind(character = c("\\s*\\[[^\\)]+\\]", "\\&", "\\$", "\\%", "\\@"), replacement = c("", "and", "dollar", "percent", "at")), stringsAsFactors = F)
crsp[, cleanname := clean_strings(name, sp_char_words = sp_char_db, common_words = common_abbr_db, remove_char = c("/", "-", "'"))]



#-----------------------
# Load the logit object
#-----------------------
logit_est <- readRDS("data/logit_est_ds_crsp.rds")

# set up tier ------------------------------------------------------------------
tier_list <- list(
  exact_unfilter = list(
    match_type = "exact"
  ),
  wgt_jaccard_unfilter = list(
    match_type = "fuzzy",
    fuzzy_settings = list(
      method = "wgt_jaccard",
      maxDist = .05,
      nthread = 22
    ),
    filter = NULL
  ),
  fuzzy_unfiltered = list(
    match_type = "fuzzy",
    fuzzy_settings = list(
      method = "jw",
      maxDist = .05,
      p = .1,
      nthread = 22
    )
  )

)

ds_raw[, `:=`(name = cleanname, zip = ZipCode, state = HSTATE, city = City, sic = PrimarySICCode)]
crsp[, `:=`(city = str_to_lower(hcity), state = STATE, zip = haddzip, sic = hsic, name = cleanname)]
# make it way smaller to test
full_tier_match <- tier_match(ds_raw, crsp,
  by = "cleanname", suffixes = c("_ds", "_crsp"), takeout = "neither",
  unique_key_1 = "FacilityID", unique_key_2 = "crsp_id", tiers = tier_list, allow.cartesian = T,
  score_settings = build_score_settings(score_var_both = "cleanname",
wgts = 1,
score_type = "stringdist"))

# full_tier_match$matches[matchscore != 1]
write_fst(full_tier_match$matches, "data/three_tier_ds_crsp_match.fst")
write_fst(full_tier_match$match_evaluation, "data/three_tier_ds_crsp_evaluation.fst")
matches_threetier <- read_fst("data/three_tier_ds_crsp_match.fst", as.data.table = T)
setkey(ds_raw, FacilityID)
ds_nomatch <- ds_raw[!FacilityID %in% matches_threetier[, FacilityID]]
# full_tier_match$

unique_ds_nomatch <- ds_nomatch[, .(name, city, state, zip, sic, FacilityID)] %>%
  unique(by = c("name", "city", "state", "zip", "sic"))
unique_crsp <- crsp[, .(name, city, state, zip, sic, crsp_id)] %>%
  unique(by = c("name", "city", "state", "zip", "sic"))
multivar_match_results <- merge_plus(unique_ds_nomatch, unique_crsp,
  suffixes = c("_ds", "_crsp"),
  unique_key_1 = "FacilityID", unique_key_2 = "crsp_id", allow.cartesian = T,
  by = c("name", "city", "state", "zip", "sic"),
  match_type = "multivar",
  multivar_settings = list(
    logit = logit_est, missing = F, wgts = NULL,
    compare_type = c("stringdist", "indicator", "indicator", "indicator", "indicator"),
    blocks = NULL, blocks.x = NULL, blocks.y = NULL,
    top = 1, threshold = .90, nthread = 6
  )
)

write_fst(multivar_match_results$matches, "data/multivar_logit_ds_crsp.fst")

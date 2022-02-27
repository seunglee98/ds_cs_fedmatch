library(data.table)
library(readxl)
library(boot)
library(usmap)
library(stringdist)
library(DBI)
library(maps)
library(dbplyr)
library(RPostgres)
library(tidyverse)
library(fedmatch)  # load matching package  -- may be from different file
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
# names(all_ds)
# names(ds_raw)
# ds_raw <- readRDS("ds_cs_match_rr/data/dealscan_WDM.rds")
# setDT(ds_raw)
#ds_initial_size <- nrow(ds_raw)

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
#ds_raw
## check duplicates
#any(duplicated(ds_raw$FacilityID))

#ds_final_size <- dim(ds_raw)[1] # final ds size
#ds_pct_keep <- 100 * ds_final_size / ds_initial_size
#ds_pct_keep_us <- 100 * ds_final_size / ds_initial_us
setDT(ds_raw)
#ds_initial_size <- nrow(ds_raw)


## Lower case cities
ds_raw[, City := tolower(City)]

# convert state names to abbreviations dealscan
ds_raw[, STATE := fifelse(State == "Washington, D.C.", "Washington D.C.", State)]
ds_raw[, HSTATE := fips(State)]
common_abbr_db <- read.csv("data/common_abbr.csv", stringsAsFactors = F)
sp_char_db <- data.frame(cbind(character = c("\\s*\\[[^\\)]+\\]", "\\&", "\\$", "\\%", "\\@"), replacement = c("", "and", "dollar", "percent", "at")), stringsAsFactors = F)
ds_raw[, cleanname := clean_strings(Company, sp_char_words = sp_char_db, common_words = common_abbr_db, remove_char = c("/", "-", "'"))]

# set names for match analysis
ds_raw[, `:=`(city = City,
              zipcode = ZipCode,
              sic = PrimarySICCode)]
# ds_raw[, sic]
ds_match_vars <- c("cleanname", "city", "HSTATE", "zipcode", "sic")

setnames(ds_raw, old = ds_match_vars, new = paste(ds_match_vars, "_ds", sep = ""))

## use row numbers for draws
ds_raw[, rowid_ds := row.names(ds_raw)]

## check duplicates
any(duplicated(ds_raw$FacilityID))


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

crsp <- crsp_hist
setDT(crsp)

## create an id
crsp[, crsp_id := .GRP, by = list(gvkey, d_dt_start)]
#any(duplicated(crsp$crsp_id))

# set the name
crsp[, name := fifelse(is.na(hconml), hconm, hconml)]

## Lower case cities
crsp[, hcity := tolower(hcity)]

## convert states to fips
crsp[, STATE := fips(hstate)]

## clean company names
common_abbr_db <- read.csv("data/common_abbr.csv", stringsAsFactors = F)
sp_char_db <- data.frame(cbind(character = c("\\s*\\[[^\\)]+\\]", "\\&", "\\$", "\\%", "\\@"), replacement = c("", "and", "dollar", "percent", "at")), stringsAsFactors = F)
crsp[, cleanname := clean_strings(name, sp_char_words = sp_char_db, common_words = common_abbr_db, remove_char = c("/", "-", "'"))]

## set names for match analysis
crsp_match_vars <- c("cleanname", "hcity", "STATE", "haddzip", "hein", "hsic")
setnames(crsp, old = crsp_match_vars, new = paste(crsp_match_vars, "_crsp", sep = ""))

## use row numbers for draws
crsp[, rowid_crsp := row.names(crsp)]

## check duplicates
any(duplicated(crsp[, list(gvkey, d_dt_start)]))

#-------------------
# Read roberts data
#-------------------

# roberts <- readRDS("/data/fedmatch/raw_data/data/roberts.rds")
roberts <- read_excel("data/chava_roberts_match.xlsx", sheet = "link_data")
setDT(roberts)
#any(duplicated(roberts$facid))
roberts[, roberts := 1]
roberts[, FacilityID := as.integer(facid)]
roberts[, gvkey := as.character(sprintf("%06.0f", gvkey))]
#roberts
setnames(roberts, old = "gvkey", new = "gvkey_roberts")

#-------------------------
# Build the logit formula
#-------------------------
compare_vars <- c("name_compare", "state_compare", "sic_compare")
missing_vars <- c("city_missing", "state_missing", "zip_missing", "sic_missing")
depvar <- "match"

f_base <- paste(depvar, "~", paste(compare_vars, collapse = "+"))
f_missing <- paste(depvar, "~", paste(c(compare_vars, missing_vars), collapse = "+"))

#-------------------------
# Draw random match pairs
#-------------------------
set.seed(1234)
n.draws <- 1e6
ds_draw <- ds_raw[sample(1:nrow(ds_raw), n.draws, replace = T), list(FacilityID, FacilityStartDate, cleanname_ds, city_ds, HSTATE_ds, zipcode_ds, sic_ds, rowid_ds)]
ds_draw[, match_id := row.names(ds_draw)]
crsp_draw <- crsp[sample(1:nrow(crsp), n.draws, replace = T), list(gvkey, d_dt_start, d_dt_end, cleanname_crsp, hcity_crsp, STATE_crsp, haddzip_crsp, hein_crsp, hsic_crsp, rowid_crsp)]
crsp_draw[, match_id := row.names(crsp_draw)]

draws <- merge(ds_draw, crsp_draw, by = "match_id", all = T)
#nrow(draws)

## merge roberts
draws <- merge(draws, roberts[, list(FacilityID, gvkey_roberts)], by = "FacilityID", all.x = T)
# draws
#-------------------
# Build comparisons
#-------------------

## outcome variable
draws[, match := fifelse(gvkey == gvkey_roberts & !is.na(gvkey) & !is.na(gvkey_roberts), 1, 0)]
#table(draws$match)

## name match (jw distance)
draws[, name_compare := 1 - stringdist(cleanname_ds, cleanname_crsp, method = "jw", p = 0.1)]
# draws[name_compare == 1]
## city
draws[, city_compare := fifelse(city_ds == hcity_crsp & !is.na(city_ds) & !is.na(hcity_crsp), 1, 0)]
draws[, city_missing := fifelse(is.na(city_ds) | is.na(hcity_crsp), 1, 0)]

## state
draws[, state_compare := fifelse(HSTATE_ds == STATE_crsp & !is.na(HSTATE_ds) & !is.na(STATE_crsp), 1, 0)]
draws[, state_missing := fifelse(is.na(HSTATE_ds) | is.na(STATE_crsp), 1, 0)]

## zipcode
draws[, zip_compare := fifelse(zipcode_ds == haddzip_crsp & !is.na(zipcode_ds) & !is.na(haddzip_crsp), 1, 0)]
draws[, zip_missing := fifelse(is.na(zipcode_ds) | is.na(haddzip_crsp), 1, 0)]

## industry code
draws[, sic_compare := fifelse(sic_ds == hsic_crsp & !is.na(sic_ds) & !is.na(hsic_crsp), 1, 0)]
draws[, sic_missing := fifelse(is.na(sic_ds) | is.na(hsic_crsp), 1, 0)]
#draws
#----------------------
# Baseline regressions
#----------------------

## baseline
base <- glm(formula = f_base, data = draws, family = "binomial")
# base_fixest <- fixest::feglm(as.formula(f_base), data = draws, family = "binomial")
# base_fixest %>% class()

#summary(base)
# f_base
## with missing values
# base_missing = glm(formula=f_missing, data=draws, family="binomial")

#---------------------------
# output parameters and SEs
#---------------------------
results <- data.table(coef = row.names(coef(summary(base))), coef(summary(base)))
setnames(results, old = c("coef", "Estimate", "Std. Error", "z value", "Pr(>|z|)"), new = c("coef", "b", "se", "z", "p_value"))
print(results)


# results_missing = data.table(coef = row.names(coef(summary(base_missing))), coef(summary(base_missing)))
# setnames(results_missing, old=c("coef", "Estimate", "Std. Error", "z value", "Pr(>|z|)"), new=c("coef", "b", "se", "z", "p_value"))
# print(results_missing)

#-----------------------
# save the logit object
#-----------------------
saveRDS(base, "data/logit_est_ds_crsp.rds")


# saveRDS(base_missing, "./data/logit_est_missing.rds")

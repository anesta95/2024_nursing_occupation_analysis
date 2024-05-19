library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(purrr)
library(janitor)
library(tidyr)

### FUNCTIONS ###
create_index_col <- function(vec, index_type = 100) {
  # Things this function needs to do:
  # 1. Check if `vec` is a numeric vector, if not, throw an error.
  # 2. Create a new vector of either a 100 base or 0 based index depending on
  # the value provided in the `index_type` argument.
  # 3. Return new index vector.
  
  if (!is.vector(vec) | length(vec) < 2 | !is.numeric(vec)) {
    stop("The supplied object to the `vec` argument object is not a numeric vector of at least two items",
         call. = T)
  }
  
  if (!(index_type %in% c(0, 100))) {
    stop("Please provide integer `0` or `100` as the index_type argument.")
  }
  
  if (index_type == 100L) {
    index_vec <- (vec / vec[1]) * 100
    message("Using 100-base index")
    return(index_vec)
  } else {
    index_vec <- ((vec / vec[1]) - 1) * 100
    message("Using 0-base index")
    return(index_vec)
  }
  
}

# Function to download and unzip all the OES tables data https://www.bls.gov/oes/tables.htm
get_oes_tables <- function(year) {
  url_head <- "https://www.bls.gov/oes/special-requests/"
  zip_file_name <- paste0("oesm", year, "nat.zip")
  zip_dest_file <- paste0("./data/raw_data/", zip_file_name)
  
  download.file(url = paste0(url_head, zip_file_name),
                destfile = zip_dest_file,
                method = "libcurl",
                headers = c("User-Agent" = "justanesta@protonmail.com"))
  
  Sys.sleep(0.5)
  
  zip_file_list_results <- unzip(zipfile = zip_dest_file, list = T)
  
  oes_data_file_name_df <- filter(zip_file_list_results, str_detect(Name, "\\_dl"))
  
  if (nrow(oes_data_file_name_df) != 1) {
    stop("Extracted zip file had more than one matched filename for data file.")
  }
  
  zip_data_file_name <- pull(oes_data_file_name_df, Name)
  
  unzip(zipfile = zip_dest_file,
        files = zip_data_file_name,
        junkpaths = T,
        exdir = "./data/working_data/")
  
  if (str_detect(zip_data_file_name, year, negate = T)) {
    file.rename(paste0("./data/working_data/", zip_data_file_name),
                paste0("./data/working_data/20", year, "_", zip_data_file_name))
  }
  
  message(paste0("Done with year '", year))
  Sys.sleep(runif(n = 1, min = 1, max = 60))
  
}

# Function to extract OES data from excel sheet and add year column based on
# value in the filename
extract_oes_table <- function(data_year) {
  
  data_filenames <- list.files("./data/working_data/") %>% 
    str_subset("\\_dl")
  
  filename <- str_subset(data_filenames, as.character(data_year))
  
  if (length(filename) != 1) {
    stop("Extracted excel workbook filename had more than one year match.")
  }
  
  file_excel_sheets <- excel_sheets(paste0("./data/working_data/", filename))
  data_sheet_name <- str_subset(file_excel_sheets, "\\_dl")
  
  if (length(data_sheet_name) != 1) {
    stop("Extracted excel workbook had more than one matched sheet name for data.")
  }
  
  df <- read_excel(
    path = paste0("./data/working_data/", filename),
    sheet = data_sheet_name,
    col_names = T,
    col_types = "text",
    trim_ws = T,
    progress = F,
    .name_repair = make_clean_names
  ) %>% 
    mutate(year = data_year)
  
  return(df)
}

# Function to get data on job satisfaction for nurses from NSSRN survey data.
extract_NSSRN_satisfaction <- function(year) {
  
  NSSRN_we <- read_excel(
    path = paste0("./data/working_data/Nursing_Workforce_",
                  year,
                  "_NSSRN_Dashboard_Data.xlsx"),
    sheet = "Work Environment",
    col_names = T,
    col_types = "text",
    skip = 1,
    .name_repair = make_clean_names
  )
  
  NSSRN_dissatisfied <- NSSRN_we %>% 
    filter(region == "All U.S. States",
           job_satisfaction != "All") %>% 
    select(license_type, percent, weighted_value, 
           job_satisfaction, percent_relative_standard_error) %>% 
    mutate(
      weighted_value = as.numeric(weighted_value),
      percent = as.numeric(percent),
      percent_relative_standard_error = as.numeric(percent_relative_standard_error),
      weighted_value_low_95 = weighted_value - (1.96 * (percent_relative_standard_error * weighted_value)), # Apply the PRSE to get full standard errors
      weighted_value_high_95 = weighted_value + (1.96 * (percent_relative_standard_error * weighted_value))
    ) %>% 
    group_by(license_type) %>% 
    mutate(percent_low_95 = weighted_value_low_95 / sum(weighted_value_low_95),
           percent_high_95 = weighted_value_high_95 / sum(weighted_value_high_95)) %>% 
    filter(job_satisfaction %in% c("Extremely dissatisfied", "Moderately dissatisfied")) %>% 
    summarize(license_type = unique(license_type),
              percent = sum(percent) * 100,
              percent_low_95 = sum(percent_low_95) * 100,
              percent_high_95 = sum(percent_high_95) * 100) %>% 
    mutate(license_type = c("APRNs",
                            "RNs & APRNs",
                            "NPs",
                            "RNs"),
           year = year) %>% 
    filter(license_type != "RNs & APRNs") %>% 
    pivot_wider(id_cols = year, names_from = license_type, values_from = c("percent", # Pivot wider to get in Datawrapper visual format
                                                                           "percent_low_95",
                                                                           "percent_high_95")) %>% 
    rename_with(.cols = matches("percent_"), .fn = ~str_remove(.x, "percent_"))
  
  return(NSSRN_dissatisfied)
  
}

# Function to get data on burnout for nurses from NSSRN survey data.
extract_NSSRN_burnout <- function(provided_year) {
  survey_year <- sym(paste0("frequency_of_burnout_", provided_year))
  
  NSSRN_covid_yr <- NSSRN_2022_covid %>% 
    filter(license_type == "RNs (not APRNs)",
           {{ survey_year }} != "All") %>% 
    select(all_of(survey_year), weighted_value, 
           percent, percent_relative_standard_error) %>% 
    mutate(
      weighted_value = as.numeric(weighted_value),
      percent = as.numeric(percent) * 100,
      percent_relative_standard_error = as.numeric(percent_relative_standard_error),
      weighted_value_low_95 = weighted_value - (1.96 * (percent_relative_standard_error * weighted_value)), # Apply the PRSE to get full standard errors
      weighted_value_high_95 = weighted_value + (1.96 * (percent_relative_standard_error * weighted_value)),
      percent_low_95 = (weighted_value_low_95 / sum(weighted_value_low_95)) * 100,
      percent_high_95 = (weighted_value_high_95 / sum(weighted_value_high_95)) * 100,
      year = provided_year
    ) %>% 
    select(all_of(survey_year), year, percent, percent_low_95, percent_high_95) %>% 
    pivot_wider(id_cols = year, names_from = {{ survey_year }}, values_from = c("percent", # Pivot wider to get in Datawrapper visual format
                                                                                "percent_low_95",
                                                                                "percent_high_95")) %>% 
    rename_with(.cols = matches("percent_"), .fn = ~str_remove(.x, "percent_"))
  
  return(NSSRN_covid_yr)
  
}

# Function to get data on future retirement plans for nurses from NSSRN survey data.
extract_NSSRN_retirement <- function(provided_year) {
  
  NSSRN_emp <- read_excel(
    path = paste0("./data/working_data/Nursing_Workforce_",
                  provided_year,
                  "_NSSRN_Dashboard_Data.xlsx"),
    sheet = "Employment",
    col_names = T,
    col_types = "text",
    skip = 1,
    .name_repair = make_clean_names
  )
  
  
  NSSRN_retirement_yr <- NSSRN_emp %>% 
    filter(str_detect(license_type, "and not"),
           projected_retirement != "All",
           region == "All U.S. States",
           sex == "All",
           race_and_ethnicity == "All") %>% 
    select(projected_retirement, weighted_value, 
           percent, percent_relative_standard_error) %>% 
    mutate(
      weighted_value = as.numeric(weighted_value),
      percent = as.numeric(percent) * 100,
      percent_relative_standard_error = as.numeric(percent_relative_standard_error),
      weighted_value_low_95 = weighted_value - (1.96 * (percent_relative_standard_error * weighted_value)),
      weighted_value_high_95 = weighted_value + (1.96 * (percent_relative_standard_error * weighted_value)),
      percent_low_95 = (weighted_value_low_95 / sum(weighted_value_low_95)) * 100,
      percent_high_95 = (weighted_value_high_95 / sum(weighted_value_high_95)) * 100,
      year = provided_year
    ) %>% 
    select(projected_retirement, year, percent, percent_low_95, percent_high_95) %>% 
    pivot_wider(id_cols = year, names_from = projected_retirement, values_from = c("percent", 
                                                                                   "percent_low_95",
                                                                                   "percent_high_95")) %>% 
    rename_with(.cols = matches("percent_"), .fn = ~str_remove(.x, "percent_"))
  
  return(NSSRN_retirement_yr)
  
}

### ANALYSIS ###

oes_years <- str_pad(03:23, width = 2, side = "left", pad = "0")

# Fetching OES all data
# Getting data from yearly excel sheets on this page https://www.bls.gov/oes/tables.htm 
# since the BLS doesn't have years before 2023 in their flat file database

walk(oes_years, ~get_oes_tables(.x))

# Building total OES dataframe
# Doing backwards from 2023 to start with all column names
oes_occ_03_23 <- list_rbind(map(2023:2003, ~extract_oes_table(.x)))

# Finding the ost prevalent detailed occupations from 2023 OEWS data https://www.bls.gov/oes/current/oes_nat.htm
oews_dtl_gt_2M <- oes_occ_03_23 %>% 
  mutate(tot_emp = as.numeric(tot_emp),
         a_median = as.numeric(a_median)) %>% 
  filter(o_group == "detailed", tot_emp > 2e6, year == 2023) %>% # Filtering for only detailed occupations with >2M estimated employees in 2023
  arrange(desc(tot_emp))

# Calculating the margin of error for the occupational employment estimate
oews_dtl_gt_2M_MOE <- oews_dtl_gt_2M %>% 
  mutate(tot_emp_min_95 = tot_emp - (1.96 * (tot_emp * (as.numeric(emp_prse) / 100))),
         tot_emp_max_95 = tot_emp + (1.96 * (tot_emp * (as.numeric(emp_prse) / 100)))) %>% 
  select(occ_title, tot_emp, tot_emp_min_95, tot_emp_max_95)
  
write_csv(oews_dtl_gt_2M_MOE, "./data/clean_data/oews_dtl_gt_2M_MOE_bar_dw.csv")


# Finding the total annual median wage in 2023 among all occupations
a_median_natl_wg <- filter(oes_occ_03_23, occ_code == "00-0000", year == 2023) %>% 
  pull(a_median)

# Selecting median annual earnings for occupations with >2M estimated employees in 2023
oews_dtl_gt_2M_WG <- oes_occ_03_23 %>% 
  mutate(tot_emp = as.numeric(tot_emp),
         a_median = as.numeric(a_median),
         a_median_natl = a_median_natl_wg) %>% 
  filter(o_group == "detailed", tot_emp > 2e6, year == 2023) %>% 
  arrange(desc(tot_emp)) %>% 
  select(occ_title, a_median, a_median_natl)

write_csv(oews_dtl_gt_2M_WG, "./data/clean_data/oews_dtl_gt_2M_WG_bar_dw.csv")

# Getting Registered Nurse, Healthcare Diagnosing or Treating Practitioners,
# and general occupation employment and wage data

# BLS OES Occupation Codes from here: https://download.bls.gov/pub/time.series/oe/oe.occupation
# 000000	All Occupations
# 290000	Healthcare Practitioners and Technical Occupations
# 291141	Registered Nurses

desired_occs <- c("00-0000", "29-0000", "29-1141")

# Finding 2012 to 2023 change in employment for all occupations, healthcare occupations,
# and nurses
healthcare_nurses_total_employment <- oes_occ_03_23 %>% 
  filter(occ_code %in% desired_occs, year >= 2012) %>% # Nurse occupation data starts in 2012 
  mutate(
    tot_emp = as.numeric(tot_emp),
    emp_prse = as.numeric(emp_prse) / 100) %>% 
  rowwise() %>% 
  mutate(
    lower_95 = tot_emp - (1.96 * (tot_emp * emp_prse)), # Apply the PRSE to get full standard errors
    upper_95 = tot_emp + (1.96 * (tot_emp * emp_prse))
    ) %>% 
  select(year, occ_code, tot_emp, lower_95, upper_95) %>% 
  arrange(year) %>% 
  pivot_wider(names_from = occ_code, # Pivot wider to get in Datawrapper visual format
              values_from = c(tot_emp, lower_95, upper_95)) %>% 
  mutate(across(-year, ~create_index_col(.x, index_type = 0))) %>% 
  rename_with(.cols = matches("tot_emp"), .fn = ~str_remove(.x, "tot_emp_")) %>% 
  rename_with(.cols = -year, .fn = ~case_when(
    str_detect(.x, "00-0000") ~ str_replace(.x, "00-0000", "All Occupations"),
    str_detect(.x, "29-0000") ~ str_replace(.x, "29-0000", "Healthcare Practitioners and Technical Occupations"),
    str_detect(.x, "29-1141") ~ str_replace(.x, "29-1141", "Registered Nurses"),
  ))

write_csv(healthcare_nurses_total_employment, "./data/clean_data/oews_healthcare_nurses_total_employment_line_dw.csv")

# Finding 2012 to 2023 change in median annual wages for all occupations, healthcare occupations,
# and nurses
healthcare_nurses_total_wages <- oes_occ_03_23 %>% 
  filter(occ_code %in% desired_occs, year >= 2012) %>% # Nurse occupation data starts in 2012 
  mutate(a_median = as.numeric(a_median)) %>% 
  select(year, occ_code, a_median) %>% 
  arrange(year) %>% 
  pivot_wider(names_from = occ_code,
              values_from = a_median) %>% 
  mutate(across(-year, ~create_index_col(.x, index_type = 0))) %>% 
  rename_with(.cols = matches("tot_emp"), .fn = ~str_remove(.x, "tot_emp_")) %>% 
  rename_with(.cols = -year, .fn = ~case_when(
    str_detect(.x, "00-0000") ~ str_replace(.x, "00-0000", "All Occupations"),
    str_detect(.x, "29-0000") ~ str_replace(.x, "29-0000", "Healthcare Practitioners and Technical Occupations"),
    str_detect(.x, "29-1141") ~ str_replace(.x, "29-1141", "Registered Nurses"),
  ))

write_csv(healthcare_nurses_total_wages, "./data/clean_data/oews_healthcare_nurses_total_wages_line_dw.csv")

# Now looking at BLS Employment Projections data
# to see occupations with the most job growth, median annual wages, 
# and associated educational, career background, and on-the-job training needed.
# Data from here https://www.bls.gov/emp/ind-occ-matrix/occupation.xlsx and 
# here https://data.bls.gov/projections/occupationProj
occ_most_job_growth_22 <- read_excel(
  path = "./data/working_data/occupation.xlsx",
  sheet = "Table 1.4",
  col_names = T,
  col_types = "text",
  trim_ws = T,
  progress = F,
  skip = 1,
  n_max = 31,
  .name_repair = make_clean_names
)

occ_emp_proj_22 <- read_csv(
  file = "./data/working_data/Employment Projections.csv",
  col_names = T,
  col_types = cols(.default = col_character()),
  name_repair = make_clean_names
)

# Need to clean up some of the dirty data from the "One Screen" BLS look
# that has education, work experience, and training requirements
occ_emp_proj_22_edu_expr_train <- occ_emp_proj_22 %>% 
  mutate(occupation_code = str_extract(occupation_code, "\\d{2}\\-\\d{4}")) %>% 
  select(occupation_code, typical_entry_level_education,
         work_experience_in_a_related_occupation,
         typical_on_the_job_training)

occ_most_job_growth_22_chg_wg <- occ_most_job_growth_22 %>% 
  select(x2022_national_employment_matrix_title,
         x2022_national_employment_matrix_code,
         employment_change_numeric_2022_32,
         employment_change_percent_2022_32,
         median_annual_wage_2023_1)

# Join the projections and education, work experience and training requirements
# dataframes by occupation code
occ_job_growth_wg_edu_expr_train <- inner_join(occ_most_job_growth_22_chg_wg, occ_emp_proj_22_edu_expr_train,
           by = c("x2022_national_employment_matrix_code" = "occupation_code")) %>% 
  mutate(employment_change_numeric_2022_32 = as.numeric(employment_change_numeric_2022_32) * 1000) %>% 
  select(-x2022_national_employment_matrix_code)

names(occ_job_growth_wg_edu_expr_train) <- c("Occupation",
                                             "Employment Change '22 to '32",
                                             "Percent Employment Change '22 to '32",
                                             "Median Annual Wage '23",
                                             "Typical Entry Level Education Needed",
                                             "Work Experience in a Related Occupation Needed",
                                             "Typical on-the-job Training Needed")

write_csv(occ_job_growth_wg_edu_expr_train,
          "./data/clean_data/occ_job_growth_wg_edu_expr_train_dw_table.csv")

# Next, Nurse Survey Data Analysis
# Getting Nurse dissatisfaction by nurse type
NSSRN_dissatisfied_18_22 <- list_rbind(map(c(2018, 2022), ~extract_NSSRN_satisfaction(.x)))

write_csv(NSSRN_dissatisfied_18_22,
          "./data/clean_data/NSSRN_dissatisfied_18_22_dw_line.csv")

# Now RN burnout by year
NSSRN_2022_covid <- read_excel(
  path = paste0("./data/working_data/Nursing_Workforce_",
                2022,
                "_NSSRN_Dashboard_Data.xlsx"),
  sheet = "COVID & Burnout",
  col_names = T,
  col_types = "text",
  skip = 1,
  .name_repair = make_clean_names
)

NSSRN_burnout_19_21 <- list_rbind(map(2019:2021, ~extract_NSSRN_burnout(.x)))
  
write_csv(NSSRN_burnout_19_21,
          "./data/clean_data/NSSRN_burnout_19_21_dw_line.csv")

# Now RN top reasons for leaving work during the pandemic
NSSRN_leaving_22 <- NSSRN_2022_covid %>% 
  filter(license_type == "RNs (not APRNs)", 
         reasons_for_leaving_work_during_pandemic != "All") %>% 
  mutate(percent = as.numeric(percent) * 100) %>% 
  arrange(desc(percent)) %>% 
  select(reasons_for_leaving_work_during_pandemic,
         percent)
  
write_csv(NSSRN_leaving_22,
          "./data/clean_data/NSSRN_leaving_22_dw_bar.csv")

# Now RN future retirement plans
NSSRN_retirement_18_22 <- list_rbind(map(c(2018, 2022), ~extract_NSSRN_retirement(.x)))

write_csv(NSSRN_retirement_18_22,
          "./data/clean_data/NSSRN_retirement_18_22_dw_line.csv")

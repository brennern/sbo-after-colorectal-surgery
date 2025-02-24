library(targets)
# library(crew)

# Functions ---------------------------------------------------------------
#source("R/functions.R")

# Target options-----------------------------------------------------------

# These packages are loaded specifically for the pipeline
# To load packages to work scripts or console, use the 'packages.R' file
tar_option_set(
  ## Packages ----
  packages = c(
    "tidyverse",
    "readr",
    "haven",
    "gtsummary"
  ),
  ## Storage ----
  # format = "qs", # 'Quick serialization'
  ## Workers ----
  # controller = crew_controller_local(workers = 3)
  # Parallel backend is registered in 'packages.R' file
)

# Targets Pipeline --------------------------------------------------------

list(
  # Load ----
  tar_target(
    dat_2022_file,
    "data/puf_tar_col_2022.sas7bdat",
    format = "file"
  ),
  tar_target(
    dat_2022,
    read_sas(dat_2022_file) |> 
      rename(
        CaseID = CASEID
      )
  ),
  tar_target(
    dat2_2022_file,
    "data/acs_nsqip_puf22.sas7bdat",
    format = "file"
  ),
  tar_target(
    dat2_2022,
    read_sas(dat2_2022_file) |> 
      select(CPT, STILLINHOSP, RETORPODAYS, READMPODAYS1, PUFYEAR, CaseID, SEX, RACE_NEW, ETHNICITY_HISPANIC, Age, HEIGHT, WEIGHT, HEMO, DIABETES, SMOKE, HXCOPD, HXCHF, HYPERMED, RENAFAIL, DIALYSIS, DISCANCR, STEROID, BLEEDDIS, TRANSFUS, PRSEPIS, PRALBUM, ASACLAS, OPTIME, SSSIPATOS, DSSIPATOS, OSSIPATOS, PNAPATOS, VENTPATOS, UTIPATOS, SEPSISPATOS, SEPSHOCKPATOS, CASETYPE, OP_APPROACH, ROBOT_USED, UNPLANNED_CONV_OPEN, HAND_OPEN_ASSIST, YRDEATH, TOTHLOS, DOpertoD, PREOP_COVID, POSTOP_COVID, PODIAGTX10, PODIAG10, RETURNOR, REOPOR1ICD101, READMRELATED1, READMRELICD101, SUPINFEC, WNDINFD, ORGSPCSSI, DEHIS, OUPNEUMO, REINTUB, PULEMBOL, FAILWEAN, RENAINSF, OPRENAFL, URNINFEC, CNSCVA, CDARREST, CDMI, OTHBLEED, OTHDVT, OTHSYSEP, OTHSESHOCK) |> 
      filter(CPT %in% c(44204:44208, 44210:44213, 45395, 45397, 44140, 44160, 44143, 44145, 44146, 44150, 44155, 44157, 44158))
  ),
  tar_target(
    dat3_2022,
    dat2_2022 |> 
      mutate(
        RACE_R = case_when(
          ETHNICITY_HISPANIC == "Yes" ~ "Hispanic",
          RACE_NEW == "Native Hawaiian or Pacific Islander" ~ "Non-Hispanic NHPI",
          RACE_NEW == "White" ~ "Non-Hispanic White",
          RACE_NEW == "Black or African American" ~ "Non-Hispanic Black",
          RACE_NEW == "Asian" ~ "Non-Hispanic Asian",
          RACE_NEW == "American Indian or Alaska Native" ~ "Non-Hispanic AIAN",
          RACE_NEW == "Unknown/Not Reported" ~ NA_character_,
          str_detect(RACE_NEW, "Native Hawaiian or Other Pacific Islander") ~ "Non-Hispanic NHPI",
          str_detect(RACE_NEW, "American Indian or Alaska Native") ~ "Non-Hispanic AIAN",
          str_detect(RACE_NEW, "Black or African American") ~ "Non-Hispanic Black",
          str_detect(RACE_NEW, "Asian") ~ "Non-Hispanic Asian",
          str_detect(RACE_NEW, "Race combinations with low frequencye|Some Other Race") ~ NA_character_
        ) |> 
          factor(levels = c("Non-Hispanic White", "Non-Hispanic Black", "Non-Hispanic Asian", "Non-Hispanic AIAN", "Non-Hispanic NHPI", "Hispanic"))
      ) |> 
      mutate(
        CPT_NEW = case_when(
          CPT == 44204 ~ "Lap partial colectomy",
          CPT == 44205 ~ "Lap partial colectomy (removal of TI)",
          CPT == 44206 ~ "Lap partial colectomy with colostomy (Hartmann)",
          CPT == 44207 ~ "Lap LAR",
          CPT == 44208 ~ "Lap LAR with colostomy",
          CPT == 44210 ~ "Lap total colectomy (w/o rectum)",
          CPT == 44211 ~ "Lap total colectomy (w/rectum, anast, loop ileostomy)",
          CPT == 44212 ~ "Lap total colectomy (w/rectum, with end ileostomy)",
          CPT == 44213 ~ "Lap mobilization of splenic flexure",
          CPT == 45395 ~ "Lap APR with colostomy",
          CPT == 45397 ~ "Lap protectomy with coloanal anast, reservoir, diversion",
          CPT == 44140 ~ "Partial colectomy",
          CPT == 44160 ~ "Partial colectomy (removal of TI)",
          CPT == 44143 ~ "Partial colectomy with colostomy (Hartmann)",
          CPT == 44145 ~ "LAR",
          CPT == 44146 ~ "LAR with colostomy",
          CPT == 44150 ~ "Total colectomy (w/o rectum)",
          CPT == 44155 ~ "Total colectomy (w/rectum, with end ileostomy)",
          CPT == 44157 ~ "Total colectomy (w/rectum, ileoanal, loop ileostomy)",
          CPT == 44158 ~ "Total colectomy (w/rectum, reservoir, loop ileostomy)"
        ),
        SEX = case_when(
          SEX == "female" ~ "Female",
          SEX == "male" ~ "Male",
          SEX == "non-binary" ~ "Non-Binary"
        ),
        SBO_REOP = ifelse(REOPOR1ICD101 %in% c("K56.609", "K91.30", "K56.5"), 1, 0),
        SBO_READM = ifelse(READMRELICD101 %in% c("K56.609", "K91.30", "K56.5"), 1, 0),
        DEATH = ifelse(YRDEATH == -99, "No Death", "Death"),
        BMI = (WEIGHT/(HEIGHT^2))*703,
        BMI_CLASS = case_when(
          BMI < 18.5 ~ "Underweight",
          BMI >= 18.5 & BMI <= 24.9 ~ "Normal range",
          BMI >= 25 & BMI <= 29.9 ~ "Overweight",
          BMI > 30 ~ "Obese"
        ),
        REOPOR1ICD101 = case_when(
          REOPOR1ICD101 == "K56.609" ~ "SBO",
          REOPOR1ICD101 == "K91.30" ~ "Post op adhesions",
          REOPOR1ICD101 == "K56.5" ~ "SBO with adhesions"
        ),
        READMRELICD101 = case_when(
          READMRELICD101 == "K56.609" ~ "SBO",
          READMRELICD101 == "K91.30" ~ "Post op adhesions",
          READMRELICD101 == "K56.5" ~ "SBO with adhesions",
          TRUE ~ NA_character_
        ),
        Age = as.numeric(Age),
        New_Age = case_when(
          Age < 65 ~ "Less than 65 years old",
          Age >= 65 ~ "65+ years old"
        ),
        Total_Operation_Time_Hours = case_when(
          OPTIME < 240 ~ "Less Than 4 Hours",
          OPTIME >= 240 & OPTIME < 480 ~ "4-8 Hours",
          OPTIME > 480 ~ "8+ Hours"
        )
      ) |> 
      rename(
        PRHEMO_A1C = HEMO
      )
  ),
  tar_target(
    dat_file,
    "data/puf_tar_col_2023.sas7bdat",
    format = "file"
  ),
  tar_target(
    dat,
    read_sas(dat_file) |> 
      rename(
        CaseID = CASEID
      )
  ),
  tar_target(
    dat2_file,
    "data/acs_nsqip_puf23.sas7bdat",
    format = "file"
  ),
  tar_target(
    dat2,
    read_sas(dat2_file) |> 
    select(CPT, STILLINHOSP, RETORPODAYS, READMPODAYS1, PUFYEAR, CaseID, SEX, RACE_NEW, ETHNICITY_HISPANIC, Age, HEIGHT, WEIGHT, PRHEMO_A1C, DIABETES, SMOKE, HXCOPD, HXCHF, HYPERMED, RENAFAIL, DIALYSIS, DISCANCR, STEROID, BLEEDDIS, TRANSFUS, PRSEPIS, PRALBUM, ASACLAS, OPTIME, SSSIPATOS, DSSIPATOS, OSSIPATOS, PNAPATOS, VENTPATOS, UTIPATOS, SEPSISPATOS, SEPSHOCKPATOS, CASETYPE, OP_APPROACH, ROBOT_USED, UNPLANNED_CONV_OPEN, HAND_OPEN_ASSIST, YRDEATH, TOTHLOS, DOpertoD, PREOP_COVID, POSTOP_COVID, PODIAGTX10, PODIAG10, RETURNOR, REOPOR1ICD101, READMRELATED1, READMRELICD101, SUPINFEC, WNDINFD, ORGSPCSSI, DEHIS, OUPNEUMO, REINTUB, PULEMBOL, FAILWEAN, RENAINSF, OPRENAFL, URNINFEC, CNSCVA, CDARREST, CDMI, OTHBLEED, OTHDVT, OTHSYSEP, OTHSESHOCK) |> 
    filter(CPT %in% c(44204:44208, 44210:44213, 45395, 45397, 44140, 44160, 44143, 44145, 44146, 44150, 44155, 44157, 44158))
  ),
  tar_target(
    dat2_all_CPT,
    read_sas(dat2_file) |> 
      select(CPT, PUFYEAR, CaseID, SEX, RACE_NEW, ETHNICITY_HISPANIC, Age, HEIGHT, WEIGHT, PRHEMO_A1C, DIABETES, SMOKE, HXCOPD, HXCHF, HYPERMED, RENAFAIL, DIALYSIS, DISCANCR, STEROID, BLEEDDIS, TRANSFUS, PRSEPIS, PRALBUM, ASACLAS, OPTIME, SSSIPATOS, DSSIPATOS, OSSIPATOS, PNAPATOS, VENTPATOS, UTIPATOS, SEPSISPATOS, SEPSHOCKPATOS, CASETYPE, OP_APPROACH, ROBOT_USED, UNPLANNED_CONV_OPEN, HAND_OPEN_ASSIST, YRDEATH, TOTHLOS, DOpertoD, PREOP_COVID, POSTOP_COVID, PODIAGTX10, PODIAG10, RETURNOR, REOPOR1ICD101, READMRELATED1, READMRELICD101, SUPINFEC, WNDINFD, ORGSPCSSI, DEHIS, OUPNEUMO, REINTUB, PULEMBOL, FAILWEAN, RENAINSF, OPRENAFL, URNINFEC, CNSCVA, CDARREST, CDMI, OTHBLEED, OTHDVT, OTHSYSEP, OTHSESHOCK)
  ),
  tar_target(
    dat3,
    dat2 |> 
      mutate(
        RACE_R = (case_when(
          ETHNICITY_HISPANIC == "Yes" ~ "Hispanic",
          RACE_NEW == "Native Hawaiian or Pacific Islander" ~ "Non-Hispanic NHPI",
          RACE_NEW == "White" ~ "Non-Hispanic White",
          RACE_NEW == "Black or African American" ~ "Non-Hispanic Black",
          RACE_NEW == "Asian" ~ "Non-Hispanic Asian",
          RACE_NEW == "American Indian or Alaska Native" ~ "Non-Hispanic AIAN",
          RACE_NEW == "Unknown/Not Reported" ~ NA_character_,
          str_detect(RACE_NEW, "Native Hawaiian or Other Pacific Islander") ~ "Non-Hispanic NHPI",
          str_detect(RACE_NEW, "American Indian or Alaska Native") ~ "Non-Hispanic AIAN",
          str_detect(RACE_NEW, "Black or African American") ~ "Non-Hispanic Black",
          str_detect(RACE_NEW, "Asian") ~ "Non-Hispanic Asian",
          str_detect(RACE_NEW, "Race combinations with low frequencye|Some Other Race") ~ NA_character_
        ) |> 
          factor(levels = c("Non-Hispanic White", "Non-Hispanic Black", "Non-Hispanic Asian", "Non-Hispanic AIAN", "Non-Hispanic NHPI", "Hispanic")))
      ) |> 
      mutate(
        CPT_NEW = case_when(
          CPT == 44204 ~ "Lap partial colectomy",
          CPT == 44205 ~ "Lap partial colectomy (removal of TI)",
          CPT == 44206 ~ "Lap partial colectomy with colostomy (Hartmann)",
          CPT == 44207 ~ "Lap LAR",
          CPT == 44208 ~ "Lap LAR with colostomy",
          CPT == 44210 ~ "Lap total colectomy (w/o rectum)",
          CPT == 44211 ~ "Lap total colectomy (w/rectum, anast, loop ileostomy)",
          CPT == 44212 ~ "Lap total colectomy (w/rectum, with end ileostomy)",
          CPT == 44213 ~ "Lap mobilization of splenic flexure",
          CPT == 45395 ~ "Lap APR with colostomy",
          CPT == 45397 ~ "Lap protectomy with coloanal anast, reservoir, diversion",
          CPT == 44140 ~ "Partial colectomy",
          CPT == 44160 ~ "Partial colectomy (removal of TI)",
          CPT == 44143 ~ "Partial colectomy with colostomy (Hartmann)",
          CPT == 44145 ~ "LAR",
          CPT == 44146 ~ "LAR with colostomy",
          CPT == 44150 ~ "Total colectomy (w/o rectum)",
          CPT == 44155 ~ "Total colectomy (w/rectum, with end ileostomy)",
          CPT == 44157 ~ "Total colectomy (w/rectum, ileoanal, loop ileostomy)",
          CPT == 44158 ~ "Total colectomy (w/rectum, reservoir, loop ileostomy)"
        ),
        SEX = case_when(
          SEX == "female" ~ "Female",
          SEX == "male" ~ "Male",
          SEX == "non-binary" ~ "Non-Binary"
        ),
        SBO_REOP = ifelse(REOPOR1ICD101 %in% c("K56.609", "K91.30", "K56.5"), 1, 0),
        SBO_READM = ifelse(READMRELICD101 %in% c("K56.609", "K91.30", "K56.5"), 1, 0),
        DEATH = ifelse(YRDEATH == -99, "No Death", "Death"),
        BMI = (WEIGHT/(HEIGHT^2))*703,
        BMI_CLASS = case_when(
          BMI < 18.5 ~ "Underweight",
          BMI >= 18.5 & BMI <= 24.9 ~ "Normal range",
          BMI >= 25 & BMI <= 29.9 ~ "Overweight",
          BMI > 30 ~ "Obese"
        ),
        Age = as.numeric(Age),
        New_Age = case_when(
          Age < 65 ~ "Less than 65 years old",
          Age >= 65 ~ "65+ years old"
        ),
        Total_Operation_Time_Hours = case_when(
          OPTIME < 240 ~ "Less Than 4 Hours",
          OPTIME >= 240 & OPTIME < 480 ~ "4-8 Hours",
          OPTIME > 480 ~ "8+ Hours"
        ),
        REOPOR1ICD101 = case_when(
          REOPOR1ICD101 == "K56.609" ~ "SBO",
          REOPOR1ICD101 == "K91.30" ~ "Post op adhesions",
          REOPOR1ICD101 == "K56.5" ~ "SBO with adhesions"
        ),
        READMRELICD101 = case_when(
          READMRELICD101 == "K56.609" ~ "SBO",
          READMRELICD101 == "K91.30" ~ "Post op adhesions",
          READMRELICD101 == "K56.5" ~ "SBO with adhesions"
        ),
        READMRELICD101 = as.character(READMRELICD101)
        #GROUP = ifelse(
          #CPT %in% c(44204:44208, 44210:44213, 45395, 45397, 44140, 44160, 44143, 44145, 44146, 44150, 44155, 44157, 44158), 
          #"GROUP_1", 
          #"GROUP_2"
        #)
      )
  ),
  tar_target(
    dat3_all_CPT,
    dat2_all_CPT |> 
      mutate(
        RACE_R = case_when(
          ETHNICITY_HISPANIC == "Yes" ~ "Hispanic",
          RACE_NEW == "Native Hawaiian or Pacific Islander" ~ "Non-Hispanic NHPI",
          RACE_NEW == "White" ~ "Non-Hispanic White",
          RACE_NEW == "Black or African American" ~ "Non-Hispanic Black",
          RACE_NEW == "Asian" ~ "Non-Hispanic Asian",
          RACE_NEW == "American Indian or Alaska Native" ~ "Non-Hispanic AIAN",
          RACE_NEW == "Unknown/Not Reported" ~ NA_character_,
          str_detect(RACE_NEW, "Native Hawaiian or Other Pacific Islander") ~ "Non-Hispanic NHPI",
          str_detect(RACE_NEW, "American Indian or Alaska Native") ~ "Non-Hispanic AIAN",
          str_detect(RACE_NEW, "Black or African American") ~ "Non-Hispanic Black",
          str_detect(RACE_NEW, "Asian") ~ "Non-Hispanic Asian",
          str_detect(RACE_NEW, "Race combinations with low frequencye|Some Other Race") ~ NA_character_
        ) |> 
          factor(levels = c("Non-Hispanic White", "Non-Hispanic Black", "Non-Hispanic Asian", "Non-Hispanic AIAN", "Non-Hispanic NHPI", "Hispanic"))
      ) |> 
      mutate(
        CPT_NEW = case_when(
          CPT == 44204 ~ "Lap partial colectomy",
          CPT == 44205 ~ "Lap partial colectomy (removal of TI)",
          CPT == 44206 ~ "Lap partial colectomy with colostomy (Hartmann)",
          CPT == 44207 ~ "Lap LAR",
          CPT == 44208 ~ "Lap LAR with colostomy",
          CPT == 44210 ~ "Lap total colectomy (w/o rectum)",
          CPT == 44211 ~ "Lap total colectomy (w/rectum, anast, loop ileostomy)",
          CPT == 44212 ~ "Lap total colectomy (w/rectum, with end ileostomy)",
          CPT == 44213 ~ "Lap mobilization of splenic flexure",
          CPT == 45395 ~ "Lap APR with colostomy",
          CPT == 45397 ~ "Lap protectomy with coloanal anast, reservoir, diversion",
          CPT == 44140 ~ "Partial colectomy",
          CPT == 44160 ~ "Partial colectomy (removal of TI)",
          CPT == 44143 ~ "Partial colectomy with colostomy (Hartmann)",
          CPT == 44145 ~ "LAR",
          CPT == 44146 ~ "LAR with colostomy",
          CPT == 44150 ~ "Total colectomy (w/o rectum)",
          CPT == 44155 ~ "Total colectomy (w/rectum, with end ileostomy)",
          CPT == 44157 ~ "Total colectomy (w/rectum, ileoanal, loop ileostomy)",
          CPT == 44158 ~ "Total colectomy (w/rectum, reservoir, loop ileostomy)"
        ),
        SEX = case_when(
          SEX == "female" ~ "Female",
          SEX == "male" ~ "Male",
          SEX == "non-binary" ~ "Non-Binary"
        ),
        SBO_REOP = ifelse(REOPOR1ICD101 %in% c("K56.609", "K91.30", "K56.5"), 1, 0),
        SBO_READM = ifelse(READMRELICD101 %in% c("K56.609", "K91.30", "K56.5"), 1, 0),
        DEATH = ifelse(YRDEATH == -99, "No Death", "Death"),
        BMI = (WEIGHT/(HEIGHT^2))*703,
        BMI_CLASS = case_when(
          BMI < 18.5 ~ "Underweight",
          BMI >= 18.5 & BMI <= 24.9 ~ "Normal range",
          BMI >= 25 & BMI <= 29.9 ~ "Overweight",
          BMI > 30 ~ "Obese"
        ),
        Age = as.numeric(Age),
        Total_Operation_Time_Hours = case_when(
          OPTIME < 240 ~ "Less Than 4 Hours",
          OPTIME >= 240 & OPTIME < 480 ~ "4-8 Hours",
          OPTIME > 480 ~ "8+ Hours"
        ),
        REOPOR1ICD101 = case_when(
          REOPOR1ICD101 == "K56.609" ~ "SBO",
          REOPOR1ICD101 == "K91.30" ~ "Post op adhesions",
          REOPOR1ICD101 == "K56.5" ~ "SBO with adhesions"
        ),
        READMRELICD101 == case_when(
          READMRELICD101 == "K56.609" ~ "SBO",
          READMRELICD101 == "K91.30" ~ "Post op adhesions",
          READMRELICD101 == "K56.5" ~ "SBO with adhesions"
        ),
        GROUP = ifelse(
        CPT %in% c(44204:44208, 44210:44213, 45395, 45397, 44140, 44160, 44143, 44145, 44146, 44150, 44155, 44157, 44158), 
        "GROUP_1", 
        "GROUP_2"
        ),
        GROUP = as.character(GROUP)
      )
  ),
  tar_target(
    dat_comb,
    inner_join(dat, dat3, by = "CaseID")
  ),
  tar_target(
    dat_comb_all_CPT,
    inner_join(dat, dat3_all_CPT, by = "CaseID")
  ),
  tar_target(
    dat_comb_2022,
    inner_join(dat_2022, dat3_2022, by = "CaseID")
  ),
  tar_target(
    large_dat,
    bind_rows(dat_comb, dat_comb_2022) |> 
      mutate(
        New_Age = factor(
          New_Age,
          levels = c("Less than 65 years old", "65+ years old")
        ),
        COL_ANASTOMOTIC = as.factor(COL_ANASTOMOTIC),
        COL_ANASTOMOTIC = relevel(
          COL_ANASTOMOTIC,
          ref = "No definitive diagnosis of leak/leak related abscess"
        ),
        DIABETES = as.factor(DIABETES),
        DIABETES = relevel(
          DIABETES,
          ref = "NO"
        ),
        COL_EMERGENT = as.factor(COL_EMERGENT),
        COL_EMERGENT = relevel(
          COL_EMERGENT,
          ref = "NULL"
        ),
        ASACLAS = as.factor(ASACLAS),
        ASACLAS = relevel(
          ASACLAS,
          ref = "None assigned"
        ),
        Total_Operation_Time_Hours = as.factor(Total_Operation_Time_Hours),
        Total_Operation_Time_Hours = relevel(
          Total_Operation_Time_Hours,
          ref = "Less Than 4 Hours"
        ),
        ICD = case_when(
          PODIAG10 == "K56.609" ~ "SBO",
          PODIAG10 == "K91.30" ~ "Post op adhesions",
          PODIAG10 == "K56.5" ~ "SBO with adhesions"
        ),
        SBO = if_else(
          !is.na(REOPOR1ICD101) | !is.na(READMRELICD101),
          "SBO",
          "No SBO"
        ),
        Total_Operation_Time_Hours_v2 = case_when(
          OPTIME < 120 ~ "Less Than 2 Hours",
          OPTIME >= 120 & OPTIME < 240 ~ "2-4 Hours",
          OPTIME >= 240 & OPTIME < 360 ~ "4-6 Hours",
          OPTIME >= 360 & OPTIME < 480 ~ "6-8 Hours",
          OPTIME > 480 ~ "8+ Hours"
        ),
        Total_Operation_Time_Hours_v2 = as.factor(Total_Operation_Time_Hours_v2),
        Total_Operation_Time_Hours_v2 = relevel(
          Total_Operation_Time_Hours_v2,
          ref = "Less Than 2 Hours"
        ),
        Death_After_Operation = ifelse(DOpertoD != -99, "Death within 30 Days Post-Operation", "Alive 30 Days Post-Operation"),
        Days_To_Death = ifelse(DOpertoD == -99, NA, DOpertoD),
        Return_To_OR = ifelse(RETORPODAYS != -99, "Returned to OR", "Did Not Return to OR"),
        Days_To_OR_Return = ifelse(RETORPODAYS == -99, NA, RETORPODAYS),
        Readmission = ifelse(READMPODAYS1 != -99, "Patient Readmitted", "Patient Not Readmitted"),
        Days_To_Readmission = ifelse(READMPODAYS1 == -99, NA, READMPODAYS1),
        column_name_lower = str_to_lower(OP_APPROACH),
        column_name_lower_no_spaces = str_remove_all(column_name_lower, " "),
        Operative_Approach = case_when(
          str_detect(column_name_lower_no_spaces, "open,laparoscopic") ~ "Laparoscopic,Open",
          str_detect(column_name_lower_no_spaces, "laparoscopic") ~ "Laparoscopic",
          str_detect(column_name_lower_no_spaces, "open") ~ "Open",
          str_detect(column_name_lower_no_spaces, "|") ~ "Other"
        )
      )
  ),
  ########################################################################################################################################################################
  ########################################################################################################################################################################
  #########Descriptive Stats##############################################################################################################################################
  ########################################################################################################################################################################
  ########################################################################################################################################################################
  # Table 1
  tar_target(
    tbl_1,
    large_dat |> 
      select(SEX, New_Age, RACE_R, BMI_CLASS, Operative_Approach, REOPOR1ICD101, READMRELICD101, COL_STEROID, COL_EMERGENT, COL_CHEMO, COL_ANASTOMOTIC, COL_ILEUS, SMOKE, HXCOPD, HXCHF, HYPERMED, RENAFAIL, DIALYSIS, DISCANCR, STEROID, BLEEDDIS, TRANSFUS, PRSEPIS, PRALBUM, ASACLAS, OPTIME, Total_Operation_Time_Hours_v2, SSSIPATOS, DSSIPATOS, OSSIPATOS, PNAPATOS, VENTPATOS, UTIPATOS, SEPSISPATOS, SEPSHOCKPATOS, CASETYPE, ROBOT_USED, UNPLANNED_CONV_OPEN, HAND_OPEN_ASSIST, STILLINHOSP, Return_To_OR, Days_To_OR_Return, Readmission, Days_To_Readmission, Death_After_Operation, Days_To_Death) |> 
      tbl_summary(
        statistic = list(
          all_continuous() ~ "{mean} ± {sd}",
          all_categorical() ~ "{n} ({p}%)"
        ),
        digits = all_continuous() ~ 2,
        missing = "no"
      ) |> 
      add_n() |> 
      bold_labels() |> 
      modify_caption("**Descriptive Statistics Table of Colorectal Surgery Patients: 2022 and 2023 NSQIP Data**")
  ),
  tar_target(
    tbl_2,
    large_dat |> 
      filter(SBO %in% c("SBO")) |> 
      select(REOPOR1ICD101, READMRELICD101, BMI_CLASS, SEX, New_Age, RACE_R, COL_STEROID, COL_EMERGENT, COL_CHEMO, COL_ANASTOMOTIC, COL_ILEUS, SMOKE, HXCOPD, HXCHF, HYPERMED, RENAFAIL, DIALYSIS, DISCANCR, STEROID, BLEEDDIS, TRANSFUS, PRSEPIS, PRALBUM, ASACLAS, OPTIME, Total_Operation_Time_Hours_v2, SSSIPATOS, DSSIPATOS, OSSIPATOS, PNAPATOS, VENTPATOS, UTIPATOS, SEPSISPATOS, SEPSHOCKPATOS, CASETYPE, ROBOT_USED, UNPLANNED_CONV_OPEN, HAND_OPEN_ASSIST, STILLINHOSP, Return_To_OR, Days_To_OR_Return, Readmission, Days_To_Readmission, Death_After_Operation, Days_To_Death) |> 
      tbl_summary(
        statistic = list(
          all_continuous() ~ "{mean} ± {sd}",
          all_categorical() ~ "{n} ({p}%)"
        ),
        digits = all_continuous() ~ 2,
        missing = "no"
      ) |> 
      add_n() |> 
      bold_labels() |> 
      modify_caption("**Descriptive Statistics Table of Colorectal Surgery Patients: 2022 and 2023 NSQIP Data**")
  ),
  tar_target(
    tbl_3a,
    large_dat |> 
      filter(REOPOR1ICD101 == "SBO" | REOPOR1ICD101 == "Post op adhesions" | REOPOR1ICD101 == "SBO with adhesions" | REOPOR1ICD101 == "NULL") |> 
      select(SEX, New_Age, RACE_R, REOPOR1ICD101, BMI_CLASS, COL_STEROID, COL_EMERGENT, COL_CHEMO, COL_ANASTOMOTIC, COL_ILEUS, SMOKE, HXCOPD, HXCHF, HYPERMED, RENAFAIL, DIALYSIS, DISCANCR, STEROID, BLEEDDIS, TRANSFUS, PRSEPIS, PRALBUM, ASACLAS, OPTIME, Total_Operation_Time_Hours_v2, SSSIPATOS, DSSIPATOS, OSSIPATOS, PNAPATOS, VENTPATOS, UTIPATOS, SEPSISPATOS, SEPSHOCKPATOS, CASETYPE, ROBOT_USED, UNPLANNED_CONV_OPEN, HAND_OPEN_ASSIST, STILLINHOSP, Return_To_OR, Days_To_OR_Return, Readmission, Days_To_Readmission, Death_After_Operation, Days_To_Death) |> 
      tbl_summary(
        statistic = list(
          all_continuous() ~ "{mean} ± {sd}",
          all_categorical() ~ "{n} ({p}%)"
        ),
        digits = all_continuous() ~ 2,
        missing = "no"
      ) |> 
      add_n() |> 
      bold_labels() |> 
      modify_caption("**Descriptive Statistics Table of Colorectal Surgery Patients: 2022 and 2023 NSQIP Data**")
  ),
  tar_target(
    tbl_3b,
    large_dat |> 
      filter(READMRELICD101 == "SBO" | READMRELICD101 == "Post op adhesions" | READMRELICD101 == "SBO with adhesions" | READMRELICD101 == "NULL") |> 
      filter(is.na(REOPOR1ICD101)) |> 
      select(SEX, New_Age, RACE_R, READMRELICD101, BMI_CLASS, COL_STEROID, COL_EMERGENT, COL_CHEMO, COL_ANASTOMOTIC, COL_ILEUS, SMOKE, HXCOPD, HXCHF, HYPERMED, RENAFAIL, DIALYSIS, DISCANCR, STEROID, BLEEDDIS, TRANSFUS, PRSEPIS, PRALBUM, ASACLAS, OPTIME, Total_Operation_Time_Hours_v2, SSSIPATOS, DSSIPATOS, OSSIPATOS, PNAPATOS, VENTPATOS, UTIPATOS, SEPSISPATOS, SEPSHOCKPATOS, CASETYPE, ROBOT_USED, UNPLANNED_CONV_OPEN, HAND_OPEN_ASSIST, STILLINHOSP, Return_To_OR, Days_To_OR_Return, Readmission, Days_To_Readmission, Death_After_Operation, Days_To_Death) |> 
      tbl_summary(
        statistic = list(
          all_continuous() ~ "{mean} ± {sd}",
          all_categorical() ~ "{n} ({p}%)"
        ),
        digits = all_continuous() ~ 2,
        missing = "no"
      ) |> 
      add_n() |> 
      bold_labels() |> 
      modify_caption("**Descriptive Statistics Table of Colorectal Surgery Patients: 2022 and 2023 NSQIP Data**")
  ),
  tar_target(
    merged_table,
    tbl_merge(
      list(tbl_1, tbl_2, tbl_3a, tbl_3b),
      tab_spanner = c("Overall", "SBO", "Re-Operation", "Readmission with No Re-Operation")
    )
  ),
  
  ########################################################################################################################################################################
  ########################################################################################################################################################################
  #########Univariate Analysis############################################################################################################################################
  ########################################################################################################################################################################
  ########################################################################################################################################################################
  tar_target(
    univariate_211_reop,
    large_dat |> 
      select(SBO_REOP, SEX, New_Age, BMI_CLASS, RACE_R, DIABETES, COL_STEROID, COL_EMERGENT, COL_CHEMO, COL_ANASTOMOTIC, COL_ILEUS, SMOKE, HXCOPD, HXCHF, RENAFAIL, DIALYSIS, DISCANCR, STEROID, BLEEDDIS, TRANSFUS, PRSEPIS, PRALBUM, SSSIPATOS, DSSIPATOS, ROBOT_USED, UNPLANNED_CONV_OPEN, HAND_OPEN_ASSIST, OPTIME, Total_Operation_Time_Hours_v2, Death_After_Operation, Days_To_Death) |> 
      tbl_uvregression(
        y = SBO_REOP,
        method = glm,
        method.args = list(family = binomial),
        exponentiate = TRUE,
        pvalue_fun = \(x) style_pvalue(x, digits = 3)
      ) |> 
      bold_p() |> 
      italicize_levels()
  ),
  tar_target(
    univariate_211_readm,
    large_dat |> 
      select(SBO_READM, SEX, New_Age, BMI_CLASS, RACE_R, DIABETES, COL_STEROID, COL_EMERGENT, COL_CHEMO, COL_ANASTOMOTIC, COL_ILEUS, SMOKE, HXCOPD, HXCHF, RENAFAIL, DIALYSIS, DISCANCR, STEROID, BLEEDDIS, TRANSFUS, PRSEPIS, PRALBUM, SSSIPATOS, DSSIPATOS, ROBOT_USED, UNPLANNED_CONV_OPEN, HAND_OPEN_ASSIST, OPTIME, Total_Operation_Time_Hours_v2, Death_After_Operation, Days_To_Death) |> 
      tbl_uvregression(
        y = SBO_READM,
        method = glm,
        method.args = list(family = binomial),
        exponentiate = TRUE,
        pvalue_fun = \(x) style_pvalue(x, digits = 3)
      ) |> 
      bold_p() |> 
      italicize_levels()
  ),
  ########################################################################################################################################################################
  ########################################################################################################################################################################
  #########Multivariate Analysis##########################################################################################################################################
  ########################################################################################################################################################################
  ########################################################################################################################################################################
  tar_target(
    multivariate_reop_211,
    large_dat |> 
      select(SBO_REOP, SEX, New_Age, BMI_CLASS, RACE_R, COL_EMERGENT, COL_ANASTOMOTIC, COL_ILEUS, PRALBUM) |> 
      glm(
        SBO_REOP ~ .,
        data = _,
        family = binomial
      )
  ),
  tar_target(
    multivariate_reop_211_tbl,
    multivariate_reop_211 |> 
      tbl_regression(
        exponentiate = TRUE,
        pvalue_fun = \(x) style_pvalue(x, digits = 3)
      ) |>
      bold_p() |>
      bold_labels() |>
      italicize_levels()
  ),
  tar_target(
    multivariate_readm_211,
    large_dat |> 
      select(SBO_READM, SEX, New_Age, BMI_CLASS, RACE_R, STEROID, COL_STEROID, COL_ANASTOMOTIC, COL_ILEUS, PRSEPIS, PRALBUM, ROBOT_USED, OPTIME, Total_Operation_Time_Hours_v2) |> 
      glm(
        SBO_READM ~ .,
        data = _,
        family = binomial
      )
  ),
  tar_target(
    multivariate_readm_211_tbl,
    multivariate_readm_211 |> 
      tbl_regression(
        exponentiate = TRUE,
        pvalue_fun = \(x) style_pvalue(x, digits = 3)
      ) |>
      bold_p() |>
      bold_labels() |>
      italicize_levels()
  )
)
  
  ########################################################################################################################################################################
  ########################################################################################################################################################################
  ########################################################################################################################################################################
  ########################################################################################################################################################################
  ########################################################################################################################################################################



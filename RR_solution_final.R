# ----------------------------------------------------------------- #
# We shall do all the SHARE data preparation in one script
# ----------------------------------------------------------------- #
# packages
library(tidyverse)
library(haven)
# ----------------------------------------------------------------- #
# imputation files
imput_names  <- str_c("SHARE/imputations/",  list.files("SHARE/imputations"))
# ----------------------------------------------------------------- #
# health files
health_names <- str_c("SHARE/health/",       list.files("SHARE/health"))
# ----------------------------------------------------------------- #
# demography files
dem_names    <- str_c("SHARE/demographics/", list.files("SHARE/demographics"))
# ----------------------------------------------------------------- #
# kid files
child_names  <- str_c("SHARE/children/",     list.files("SHARE/children"))
# ----------------------------------------------------------------- #
# employment files
employ_names <- str_c("SHARE/employment/",   list.files("SHARE/employment"))
# ----------------------------------------------------------------- #
# social support files
soc_support  <- str_c("SHARE/soc_support/",  list.files("SHARE/soc_support"))
# ----------------------------------------------------------------- #
# overall generated file
gen_file     <- "SHARE/sharewX_rel9-0-0_gv_allwaves_cv_r.sav"
# ----------------------------------------------------------------- #
# We shall run read_gen function for each wave (assuming 1 to 9 waves, excl 3)
waves        <- 1:8
# We shall define age dynamically in function based on exact year
age          <- c("04", "07", "09", "11", "13", "15", "17", "20", "21", "22")
# ----------------------------------------------------------------- #
# defining a function that will take corresponding files with info
# and bind them to overall wave file for each wave separately
# this function will also "clean" data removing duplicates and unnecessary info
read_gen <- function(wave) {
  
  # ----------------------------------------------------------------- #
  # choose wave files
  age          <- age[wave]
  imput_file   <- imput_names[wave]
  health_file  <- health_names[wave]
  dem_file     <- dem_names[wave]
  child_file   <- child_names[wave]
  employ_file  <- employ_names[wave] 
  support_file <- soc_support[wave]
  
  # ----------------------------------------------------------------- #
  # read generated files
  # main source to which we shall bind
  gen_files <- read_sav(
    gen_file,
    col_select = c(
      mergeid,
      country,
      gender,
      yrbirth,
      mobirth,
      firstwave,
      firstwave_hh,
      str_c("age20",          age, sep = ""),
      str_c("agep20",         age, sep = ""),
      str_c("mergeidp",      wave, sep = ""),
      str_c("age_int_w",     wave, sep = ""),
      str_c("hhid",          wave, sep = ""),
      str_c("int_year_w",    wave, sep = ""),
      str_c("int_month_w",   wave, sep = ""),
      str_c("hhsize_w",      wave, sep = ""),
      str_c("partnerinhh_w", wave, sep = ""),
      str_c("coupleid",      wave, sep = ""),
      str_c("yrbirthp_w",    wave, sep = ""),
      str_c("mobirthp_w",    wave, sep = ""),
      str_c("deadoralive_w", wave, sep = ""),
      deceased_year,
      deceased_month,
      deceased_age
    )
  ) %>%
    # Filter based on wave-specific conditions
    # keep only years of interview for a wave 
    filter(!!sym(paste0("int_year_w",    wave)) > 0,
           # person is dead or age is known
           !!sym(paste0("age20",         age))  >= 0 |
           !!sym(paste0("age20",         age))  == -98,
           # just in case extra check for sample adequacy
           !!sym(paste0("deadoralive_w", wave)) %in% c(1, 2))
  
  # ----------------------------------------------------------------- #
  # Imputation file now
  imputation <- read_sav(
    imput_file,
    col_select = c(
      mergeid,
      htype,
      exrate,
      nursinghome,
      single,
      couple,
      partner,
      p_nrp,
      yaohm,
      yincnrp,
      ydip,
      yind,
      thinc,
      isced,
      mstat,
      nchild,
      ngrchild,
      cjs,
      ghih,
      currency,
      implicat
    )
  ) %>% 
    # implicat - multiple imputation (MI) to handle missing values
    # in variables such as income, wealth, or health status.
    # 5 implicates
    # in our case only household income !
    # we use first implicate
    filter(implicat == 1)

  # ----------------------------------------------------------------- #
  # Health file
  health <- read_sav(health_file,
                     col_select = c(
                       mergeid,
                       sphus,
                       casp,
                       num_range("euro", 1:12),
                       eurod
                     ))
  
  # ----------------------------------------------------------------- #
  # Demography file
  demography <- read_sav(dem_file,
                         col_select = c(mergeid, dn003_, 
                                        dn004_, dn005c, 
                                        dn014_, dn018_, 
                                        dn019_))
  
  # ----------------------------------------------------------------- #
  # Children file
  children <- read_sav(
    child_file,
    col_select = c(
      mergeid,
      ch001_,
      starts_with("childid"),
      ch001_,
      starts_with("ch005_"),
      starts_with("ch006_"),
      starts_with("ch007_"),
      starts_with("ch008c_"),
      starts_with("ch012_"),
      starts_with("ch013_"),
      starts_with("ch014_"),
      starts_with("ch015_"),
      starts_with("ch016_"),
      starts_with("ch017_"),
      starts_with("ch019_"),
      starts_with("ch020_"),
      ch021_,
      ch022_
    )
  )

  # ----------------------------------------------------------------- #
  # Employment file
  employment <- read_sav(employ_file, 
                         col_select = c(mergeid, 
                                        ep005_, 
                                        ep069d4))
  
  # ----------------------------------------------------------------- #  
  # Support file
  support <- read_sav(support_file,
                      col_select = c(
                        mergeid,
                        sp002_,
                        sp014_,
                        starts_with("sp016"),
                        starts_with("sp017")
                      ))
  
  # ----------------------------------------------------------------- #
  # we will create a variable age_at_year defining the point
  # at which age was evaluated
  Age <- names(gen_files)[str_detect(names(gen_files), age)][1]
  
  # ----------------------------------------------------------------- #
  # Now we shall combine all parts into one
  final <- lst(gen_files, 
               imputation,
               health, 
               demography, 
               children,
               employment, 
               support) %>%
    reduce(left_join, by = "mergeid") %>%
    # here we dynamically rename our wave specific files
    # for maximum security and binding efficienly
    # we effectively transforming wide to long format
    rename(
      int_year    := !!str_c("int_year_w",    wave),
      int_month   := !!str_c("int_month_w",   wave),
      age         := !!str_c("age20",         age),
      age_int     := !!str_c("age_int_w",     wave),
      hhsize      := !!str_c("hhsize_w",      wave),
      partnerinhh := !!str_c("partnerinhh_w", wave),
      coupleid    := !!str_c("coupleid",      wave),
      mergeidp    := !!str_c("mergeidp",      wave),
      yrbirthp_w  := !!str_c("yrbirthp_w",    wave),
      mobirthp_w  := !!str_c("mobirthp_w",    wave),
      agep        := !!str_c("agep20",        age),
      deadoralive := !!str_c("deadoralive_w", wave)
    ) %>% 
    mutate(
      age_at_year = parse_number(Age)
    )
  
  # ----------------------------------------------------------------- #
  # return our file
  return(final)
  
}

# ----------------------------------------------------------------- #
# we evaluate our function here and store the results in a named list
final_list <- map(waves, read_gen) %>% 
  set_names(paste0("wave", c(1, 2, 4:9))) 

# ----------------------------------------------------------------- #
# here we bind separate waves into one complete file 
# we also create a variable wave_nr identifying wave number
final_dt <- final_list %>% 
  bind_rows(.id = "wave_nr")

# ----------------------------------------------------------------- #
# finally saving data in .sav format
write_dta(final_dt, "rob_r_results/SHARE_RT.dta")

# ----------------------------------------------------------------- #
# read like this
# final_dt <- read_dta("rob_r_results/SHARE_RT.dta")

# ----------------------------------------------------------------- #
# auxiliary EDA function. Legacy. It has served its purpose already.
# explain_names <- function(data) {
#   tibble(name    = names(data), 
#          meaning = map_chr(data, ~ attr(.x, "label")))
# }

# ----------------------------------------------------------------- #
# END.
# ----------------------------------------------------------------- #
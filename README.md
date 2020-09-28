## Scoping the Project

#### Explore the Data & Cleaning Steps

- **I94 Immigration Data**: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from. immigration_data_sample.csv contains the sample data.
- **U.S. City Demographic Data**: This data comes from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. This data comes from the US Census Bureau's 2015 American Community Survey. 
- **Airport Code Table**: This data comes from [ourairports](http://ourairports.com/data/airports.csv). It is a simple table of airport codes and corresponding cities. It contains the list of all airport codes, the attributes are identified in datapackage description. Some of the columns contain attributes identifying airport locations, other codes (IATA, local if exist) that are relevant to identification of an airport.

##### Immigration Data
- year - 4 digit year
- month - Numeric month
- citizenship - This format shows all the valid and invalid codes for processing
- resident - This format shows all the valid and invalid codes for processing
- port - This format shows all the valid and invalid codes for processing
- arrival_date is the Arrival Date in the USA. It is a SAS date numeric field that a 
   permament format has not been applied.  Please apply whichever date format 
   works for you.
- mode - There are missing values as well as not reported (9)
- us_states - There is lots of invalid codes in this variable and the list below 
   shows what we have found to be valid, everything else goes into '99'
- depart_date is the Departure Date from the USA. It is a SAS date numeric field that 
   a permament format has not been applied.  Please apply whichever date format 
   works for you.
- age - Age of Respondent in Years
- visa_category - Visa codes collapsed into three categories
- date_added - Character Date Field - Date added to I-94 Files - CIC does not use
- visa_issued_by - Department of State where where Visa was issued - CIC does not use
- occupation - Occupation that will be performed in U.S. - CIC does not use
- arrival_flag - admitted or paroled into the U.S. - CIC does not use
- departure_flag - Departed, lost I-94 or is deceased - CIC does not use
- update_flag - Either apprehended, overstayed, adjusted to perm residence - CIC does not use
- match_flag - Match of arrival and departure records
- birth_year - 4 digit year of birth
- allowed_date - Date to which admitted to U.S. (allowed to stay until) - CIC does not use
- gender - Non-immigrant sex
- insnum - INS number
- airline - Airline used to arrive in U.S.
- admission_number - Admission Number
- flight_no - Flight number of Airline used to arrive in U.S.
- visatype - Class of admission legally admitting the non-immigrant to temporarily stay in U.S.
- id_ -Unique ID for each arrive/departure record.

#### Steps
- Generate spark session
- Process and clean **immigration_data**, including remove redundant, readable column name, unique ID and formatted filename, and generate the spark SQL schema to parquet file.
- Process and clean **airport**, merge airport code as part of combine primary key. And generate the spark SQL schema to parquet file.
- Process and clean **us_cities**, make column name more standarized and readable. And generate the spark SQL schema to parquet file.
- Process **mapping table**, including mark of Country, US states, visa code and the way of passing boarder. And generate the spark SQL schema to parquet file.
- Upload the parquet above to S3 bucket.

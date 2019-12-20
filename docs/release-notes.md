## Release Notes

A non-exhaustive list of what has changed in a more readable form than a commit history.

### v1.1.2
 - Correct typo in output definition for `CombineAll` for successful luigi pipeline

### v1.1.1
 - Updated member join in the Persistent Asthma Patients Medication Adherence measure to exclude patients without coverage in their member span

### v1.1.0

 - Added quality measure 'PCPFollowup'
   - Used to implement two non-HEDIS measures: 7-day and 14-day PCP followup rates
 - Added quality measure `Avoidance of Antibiotic Treatment in Adults With Acute Bronchitis` (AAB)
 - Fixed coverage issues and some bugs for pytest and CI.

### v1.0.0

 - Implemented utils library for the following functions:
   -  `find_elig_gaps`
   - `flag_gap_exclusions`
 - Implemented Comprehensive Diabetes Care Measure
 - Implemented Persistent Asthma Patients Medication Adherence Measure

### v0.9.1
 - Updated targets to be the most recent HEDIS average result rounded to the nearest 5.

### v0.9.0

 - Implemented Persistence of Beta-Blockers after Heart Attack Measure
 - Implemented All-cause Readmissions (PCR) Measure
 - Implemented Childhood Immunization Measure
 - Implemented Annual Monitoring for Patients on Diuretics Measure
 - Implemented Statin Therapy with Cardiovascular Disease Measure
 - Implemented Statin Therapy with Diabetes Measure

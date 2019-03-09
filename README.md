# Strava File Fixer

AWS Lambda function that modifies `.csv` files to eliminate the missing heart rate data.

Currently, it replaces each missing datapoint with the previous non-zero value.

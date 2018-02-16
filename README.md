# tap-duedil

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
 - Pulls raw data from [DueDil](https://www.duedil.com/api/docs#tag/Essentials)
 - Supports querying for companies that match given criteria
 - Fetches data from the following endpoints:
   - [company vitals](https://www.duedil.com/api/docs#tag/Essentials%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D.%7Bformat%7D%2Fget)
   - [company industries](https://www.duedil.com/api/docs#tag/Essentials%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1industries.%7Bformat%7D%2Fget)
   - [compay addresses](https://www.duedil.com/api/docs#tag/Essentials%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1addresses.%7Bformat%7D%2Fget)
   - [company descriptions](https://www.duedil.com/api/docs#tag/Essentials%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1descriptions.%7Bformat%7D%2Fget)
   - [keywords](https://www.duedil.com/api/docs#tag/Essentials%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1keywords.%7Bformat%7D%2Fget)
   - [telephone numbers](https://www.duedil.com/api/docs#tag/Essentials%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1telephone-numbers.%7Bformat%7D%2Fget)
   - [websites](https://www.duedil.com/api/docs#tag/Essentials%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1websites.%7Bformat%7D%2Fget)
   - [related names](https://www.duedil.com/api/docs#tag/Essentials%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1related-names.%7Bformat%7D%2Fget)
   - [company officers](https://www.duedil.com/api/docs#tag/Essentials%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1officers.%7Bformat%7D%2Fget)
   - [social media](https://www.duedil.com/api/docs#tag/Essentials%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1social-media-profiles.%7Bformat%7D%2Fget)
   - [shareholders](https://www.duedil.com/api/docs#tag/Ownership%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1shareholders.%7Bformat%7D%2Fget)
   - [company group (parents)](https://www.duedil.com/api/docs#tag/Ownership%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1group-parents.%7Bformat%7D%2Fget)
    - [subsidiaries](https://www.duedil.com/api/docs#tag/Ownership%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1group-subsidiaries.%7Bformat%7D%2Fget)
   - [portfolio](https://www.duedil.com/api/docs#tag/Ownership%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1portfolio-companies.%7Bformat%7D%2Fget)
   - [gazette notices](https://www.duedil.com/api/docs#tag/Ownership%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1gazette-notices.%7Bformat%7D%2Fget)
   - [related companies](https://www.duedil.com/api/docs#tag/Ownership%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1related-companies.%7Bformat%7D%2Fget)
   - [FCA authorisation](https://www.duedil.com/api/docs#tag/Ownership%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1fca-authorisations.%7Bformat%7D%2Fget)
   - [company filings](https://www.duedil.com/api/docs#tag/Ownership%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1filings.%7Bformat%7D%2Fget)
   - [company charges](https://www.duedil.com/api/docs#tag/Ownership%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1charges.%7Bformat%7D%2Fget)
   - [person of significant controls](https://www.duedil.com/api/docs#tag/Ownership%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1persons-significant-control.%7Bformat%7D%2Fget)
   - [company financials](https://www.duedil.com/api/docs#tag/Financials%2Fpaths%2F~1company~1%7BcountryCode%7D~1%7BcompanyId%7D~1financials.%7Bformat%7D%2Fget)
 - Outputs a schema for each resource

## Quick Start
#### 1. Create and source a virtualenv

```
$ virtualenv env
$ source env/bin/activate
```

#### 2. Install from source

```
$ pip install .
```

#### 3. Create a config file

You must create a JSON configuration file that looks like this:

```json
{
    "api_key": "..."
}
```

You can find your API Key in the DueDil web interface.

#### 4. Run the Tap in Discovery Mode

A script has been provided to generate a catalog.json file from the JSON Schemas provided for this tap. Run:

```
$ python get_catalog.py > catalog.json
```

to create a catalog file.

#### 5. Run the tap in Query mode

First, create a JSON file containing your company search criteria.

```json
# company-query.json
{
    "criteria": {
        "countryCodes": {
            "values": [
                "GB"
            ]
        }
    }
}
```

Next, run the tap in query mode:

```
$ tap-duedil query -q company-query.json -c config.json -p catalog.json --companies companies.txt
```

This command will create a text file called companies.txt, containing one company per line. When the tap
is run in "sync" mode, the tap will use this file to sync each endpoint for each company.

#### 6. Run the tap in Sync mode

```
$ tap-duedil sync -c config.json -p catalog.json --companies companies.txt
```

The output of `tap-duedil sync ....` can be piped to `target-stitch` to load the data into your warehouse.

A subset of streams can be selected using the `--streams` argument to `tap-duedul sync`. For more information 
on these parameters, run `tap-duedul sync -h`

---

Copyright &copy; 2017 Fishtown Analytics

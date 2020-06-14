# Sendinblue Contacts Loader

CSV contacts loader for [Sendinblue](https://sendinblue.com/).

Made for [She Loves Data](https://shelovesdata.com/).

## Configuration file format:

`config.json` goes to `/data/` folder.

```json
{
    "parameters": {
        "credentials": {
            "api_key": "<api_key>"
        },
        "debug": {
            "level": "debug"
        },
        "op": {
            "action": "getall"
        }
    }
}
```

Debug logging levels: `info`, `warning`, `debug`.
Available operations: 
- `getall` :: gets all (50 max) contacts and logs them
- `delete` :: deletes contacts
- `update` :: updates contacts
- `create` :: creates contacts

## CSV files format

CSVs should be placed in `/data/in/tables/`.
Multiple CSV files are allowed.

```csv
EMAIL,LASTNAME,FIRSTNAME,CITY,COUNTRY
blah@blah.org,Doozer,Bob,San-Francisco,USA
glob@blog.org,Pipin,Nick,Berlin,Germany
```

# Integration logos

README badges use CDN URLs (jsDelivr + Devicon) so they render on GitHub without committing binary assets.

To use **local** images instead (offline README, air-gapped docs):

1. Add files here (suggested names):

   | File | Use |
   |------|-----|
   | `mongodb.svg` | MongoDB source |
   | `dynamodb.svg` | DynamoDB source |
   | `bigquery.svg` | BigQuery destination |
   | `redshift.svg` | Redshift destination |
   | `postgresql.svg` | PostgreSQL destination |
   | `s3.svg` | S3 destination |

2. In root `README.md`, replace CDN `src` URLs with relative paths, e.g. `docs/images/mongodb.svg`.

3. Commit the binaries so clones and PyPI source distributions include them.

Official brand assets: use each vendor’s press/brand kit; Devicon/Simple Icons are community icons for documentation only.

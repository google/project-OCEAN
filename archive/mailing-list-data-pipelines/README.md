# Mailing List Data Pipelines

No maintenance intended: https://github.com/google/project-OCEAN/issues/97 

This code pulls data from multiple sources (google groups, mailman, and pipermail, see [1-raw-data](1-raw-data/)), then performs transformations [2-transform-data](2-transform-data/) and analysis ([3-analyze-data](3-analyze-data/)). 

Known issues: 

 * The Google Groups code no longer works. See https://github.com/google/project-OCEAN/issues/94
 * Dependency updates since original development may remove security issues, but may introduce bugs to pipelines.  
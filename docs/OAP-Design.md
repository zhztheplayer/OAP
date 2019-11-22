# OAP Design


* [Introduction](#introduction)
* [OAP Architecture](#oap-architecture)
* [OAP Components](#oap-components)


## Introduction

Interactive queries usually processes on a large data set but return a small portion of data filtering out with a specific condition. Customers are facing big challenges in meeting the performance requirement of interactive queries as we wants the result returned in seconds instead of tens of minutes or even hours. 

OAP (Optimized Analytic Package ) is a package for Spark to speed up interactive queries (ad-hoc queries) by utilizing index and cache technologies. By properly using index and cache, the performance of some interactive queries can possible be improved by order of magnitude.

![OAP-INTRODUCTION](./image/OAP-Introduction.PNG)

## OAP Architecture

OAP is designed to leverage the user defined indices and smart fine-grained in-memory data caching strategy for boosting Spark SQL performance on ad-hoc queries.

![OAP-ARCHITECTURE](./image/OAP-Architecture.PNG)

By using DCPMM (AEP) as index and data cache, we can provide a more cost effective solutions for high performance environment requirement.


## OAP Components
### Index 

![OAP-INDEX](./image/OAP-Index.PNG)

### Cache
Cache Aware
![CACHE-AWARE](./image/Cache-Aware.PNG)

### OAPFileFormat

![OAPFILEFORMAT](./image/OAPFileFormat.PNG)

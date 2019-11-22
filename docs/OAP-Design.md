# OAP Design


* [Introduction](#introduction)


## Introduction

Interactive queries usually processes on a large data set but return a small portion of data filtering out with a specific condition. Customers are facing big challenges in meeting the performance requirement of interactive queries as we wants the result returned in seconds instead of tens of minutes or even hours. 

OAP (Optimized Analytic Package ) is a package for Spark to speed up interactive queries (ad-hoc queries) by utilizing index and cache technologies. By properly using index and cache, the performance of some interactive queries can possible be improved by order of magnitude.

![OAP-INTRODUCTION](./docs/image/OAP-Introduction.PNG)

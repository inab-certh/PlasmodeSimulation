# PlasmodeSimulation

## Introduction

`PlasmodeSimulation` is an R-package for creating simulated versions of
healthcare databases—mapped to the OMOP Common Data Model—based on predefined
cohorts. Unlike conventional approaches that fully generate synthetic
patient-level data (demographics, diagnoses, medications, etc.), plasmode
simulations reuse real patient records and focus on simulating only the
outcomes. Specifically, after fitting outcome models to the observed data, the
package generates new outcomes for every patient. Although still in early
development, PlasmodeSimulation can already produce benchmark datasets with
known positive and negative controls (i.e., treatment–outcome pairs with
established effect sizes) to rigorously evaluate pharmacovigilance methods.

## Installation

To install the latest version from github run:

```r
remotes::install_github("inab-certh/PlasmodeSimulation")
```

## Development status

The R-package is still in early beta stage. Breaking changes may occur.

## More information

For more information consult the package website, containing function
documentation and vignettes.

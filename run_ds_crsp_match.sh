#!/bin/bash
#$ -cwd
#$ -pe onenode 8
#$ -l m_mem_free=6G
#$ -m abe
#$ -M chris.webster@frb.gov
R CMD BATCH --no-save --no-restore  wrdscloud_logit_base_estimates_ds_crsp.R
R CMD BATCH --no-save --no-restore  wrdscloud_ds_crsp_match_tiered.R

## Datasets and Inputs

Due to the large size of the data files,
they are not included in the repository.

This folder after all should contain the following directories:
1. `originals`: This directory contains the original data files
2. `batches`: This directory should contains the preprocessed data files after running all the preprocessing scripts. Inside this directory, there should be three subdirectories `1`, `2`, and `3` which correspond to the three batches of data. They should contain files with the same names as the originals, with the exception of lfb dataset, which will get merged into one file.

When writing jobs, make the input take a path to the `batches` directory. When running the job, you can specify the batch number to run on.

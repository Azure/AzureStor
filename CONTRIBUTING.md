Contributions to **AzureStor** are welcome from anyone and are best sent as pull requests on [the GitHub repository](https://github.com/cloudyr/AzureStor/). This page provides some instructions to potential contributors about how to add to the package.

 1. Contributions can be submitted as [a pull request](https://help.github.com/articles/creating-a-pull-request/) on GitHub by forking or cloning the [repo](https://github.com/cloudyr/AzureStor/), making changes and submitting the pull request.
 
 2. The cloudyr project follows [a consistent style guide](http://cloudyr.github.io/styleguide/index.html) across all of its packages. Please refer to this when editing package code.
 
 3. Pull requests should involve only one commit per substantive change. This means if you change multiple files (e.g., code and documentation), these changes should be committed together. If you don't know how to do this (e.g., you are making changes in the GitHub web interface) just submit anyway and the maintainer will clean things up.
 
 4. All contributions must be submitted consistent with the package license ([MIT](https://opensource.org/licenses/MIT)).
 
 5. Non-trivial contributions need to be noted in the `Authors@R` field in the [DESCRIPTION](https://github.com/cloudyr/AzureStor/blob/master/DESCRIPTION). Just follow the format of the existing entries to add your name (and, optionally, email address). Substantial contributions should also be noted in [`inst/CITATION`](https://github.com/cloudyr/AzureStor/blob/master/inst/CITATION).
 
 6. The cloudyr project use royxgen code and documentation markup, so changes should be made to roxygen comments in the source code `.R` files. If changes are made, roxygen needs to be run. The easiest way to do this is a command line call to: `Rscript -e devtools::document()`. Please resolve any roxygen errors before submitting a pull request.
 
 7. Please run `R CMD BUILD AzureStor` and `R CMD CHECK AzureStor_VERSION.tar.gz` before submitting the pull request to check for any errors.
 
Some specific types of changes that you might make are:

 1. Bug fixes. Great!
 
 2. Documentation-only changes (e.g., to Rd files, README, vignettes). This is great! All contributions are welcome.
 
 3. New functionality. This is fine, but should be discussed on [the GitHub issues page](https://github.com/cloudyr/AzureStor/issues) before submitting a pull request.
 
 3. Changes requiring a new package dependency should also be discussed on [the GitHub issues page](https://github.com/cloudyr/AzureStor/issues) before submitting a pull request.
 
 4. Message translations. These are very appreciated! The format is a pain, but if you're doing this I'm assuming you're already familiar with it.

Any questions you have can be opened as GitHub issues or directed to hongooi (at) microsoft.com (for questions about this package) or thosjleeper (at) gmail.com (for questions about cloudyr in general).

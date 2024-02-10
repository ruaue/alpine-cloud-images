* consider separating official Alpine Linux configuration into an overlay
  to be applied via `--custom`.

* add per-cloud documentation for importing images

* figure out `image_compression`, especially for the weird case of GCP

* clean up cloud modules now that `get_latest_imported_tags` isn't really
  needed -- AWS publish_image still uses it to make sure the imported image
  is actually there (and the right one), this can be made more specific.

* do we still need to set `ntp_server` for AWS images, starting with 3.18.4?
  _(or is this now handled via `dhcpcd`?)_

* figure out rollback / `refresh_state()` for images that are already signed,
  don't sign again unless directed to do so.

* clean up cloud modules now that `get_latest_imported_tags` isn't needed

* do we still need to set `ntp_server` for AWS images, starting with 3.18.4?
  _(or is this now handled via `dhcpcd`?)_

* figure out rollback / `refresh_state()` for images that are already signed,
  don't sign again unless directed to do so.

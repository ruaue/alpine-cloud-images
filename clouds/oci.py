from .interfaces.adapter import CloudAdapterInterface

# NOTE: This stub allows images to be built locally and uploaded to storage,
#   but code for automated importing and publishing of images for this cloud
#   publisher has not yet been written.

class OCICloudAdapter(CloudAdapterInterface):

    def get_latest_imported_tags(self, project, image_key):
        return None

    def import_image(self, ic):
        pass

    def delete_image(self, config, image_id):
        pass

    def publish_image(self, ic):
        pass

def register(cloud, cred_provider=None):
    return OCICloudAdapter(cloud, cred_provider)

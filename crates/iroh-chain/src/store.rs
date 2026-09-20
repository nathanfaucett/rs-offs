use iroh::EndpointId;

pub trait AllowedEndpointId: Send + Sync + 'static {
    fn allowed(&self, id: EndpointId) -> impl Future<Output = bool> + Send;
}

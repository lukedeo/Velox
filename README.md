<img src="img/logo.png" width="200">
--- 

**Welcome to `vēlōx!`**

Deploying and managing live machine learning models is difficult. It involves a mix of handling model versioning, hot-swapping new versions and determining version constraint satisfaction on-the-fly, and managing binary file movement either on a networked or local file system or with a cloud storage system like S3. `vēlōx` can handle this for you with a simple base class enforcing opinionated methods of handling the above problems. 


```python
@register_model(
    registered_name='foobar', 
    version='0.1.0-alpha',
    version_constraints='>=0.1.0,<0.2.0'
)
class FooBar(ManagedObject):
    def __init__(self, big_object):
        super(ManagedObject, self).__init__()
        self._big_object = big_object
    
    def _save(self, fileobject):
        pickle.dump(self, fileobject)

    @classmethod
    def _load(cls, fileobject):
        return pickle.load(fileobject)

    def predict(self, X):
        return self._big_object.predict(X)
```



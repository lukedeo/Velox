<img src="img/logo.png" width="200">

# Vēlōx

Welcome to Vēlōx! 

Deploying and managing live machine learning models is difficult. It involves a mix of handling model versioning, hot-swapping new versions and determining version constraint satisfaction on-the-fly, and managing binary file movement either on a networked or local file system or with a cloud storage system like S3. Vēlōx can handle this for you with a series of simple base classes enforcing opinionated methods of handling the above problems. 


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
```



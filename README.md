# Velox

<img src="img/logo.png" width="100">

**Welcome to `velox!`**

Deploying and managing live machine learning models is difficult. It involves a mix of handling model versioning, hot-swapping new versions and determining version constraint satisfaction on-the-fly, and managing binary file movement either on a networked or local file system or with a cloud storage system like S3. `velox` can handle this for you with a simple base class enforcing opinionated methods of handling the above problems. 

Velox provides two main utilities:

* Velox abstracts the messiness of consistent naming schemes and handling saving and loading requirements for a filesystem and for other forms of storage.
* Velox allows the ability to do a model / blob hotswap in-place for a new binary version.

## `VeloxObject` ABC 

Functionality is exposed using the `VeloxObject` abstract base class (ABC). A subclass of a `VeloxObject` needs to implement three things in order for the library to know how to manage it. 

* Your class must be defined with a `@register_model` decorator around it.
* Your class must implement a `_save` object method that takes as input a file object and does whatever is needed to save the object.
* Your class must implement a `_load` class method (with the `@classmethod` decorator) that takes as input a file object and reconstructs and returns an instance of your class.

Here is an example:


```python
@register_model(
    registered_name='foobar', 
    version='0.1.0-alpha',
    version_constraints='>=0.1.0,<0.2.0'
)
class FooBar(VeloxObject):
    def __init__(self, big_object):
        super(VeloxObject, self).__init__()
        self._big_object = big_object
    
    def _save(self, fileobject):
        pickle.dump(self, fileobject)

    @classmethod
    def _load(cls, fileobject):
        return pickle.load(fileobject)

    def predict(self, X):
        return self._big_object.predict(X)
```



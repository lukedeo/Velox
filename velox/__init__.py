#!/usr/bin/env python
# -*- coding: utf-8 -*-

u"""

<img src="https://github.com/lukedeo/Velox/raw/master/img/velox-logo.png" width=37% align="right" />

# Welcome to Velox!

Deploying and managing live machine learning models is difficult. It involves a mix of handling model versioning, hot-swapping new versions and determining version constraint satisfaction on-the-fly, and managing binary file movement either on a networked or local file system or with a cloud storage system like S3. Velox can handle this for you with a simple base class enforcing opinionated methods of handling the above problems.

Velox provides two main utilities:

* Velox abstracts the messiness of consistent naming schemes and handling saving and loading requirements for a filesystem and for other forms of storage.
* Velox allows the ability to do a model / blob hotswap in-place for a new binary version.

## Requirements
---

Velox currently only supports Python 2.7, but **we would love contributions towards Python 3 support** 😁

The main requirements are `apscheduler` for scheduling hot-swaps, `semantic_version` for version sanity, and the `futures` Python 2.7 backport. If you want to be able to work with S3, you'll need `boto3` (and a valid and properly set up AWS account).

To run the tests, you'll need the brilliant `moto` library, the `backports.tempfile` library for Python 2.7 compatibility, and `Keras` and `sckit-learn`.

For logging, simply grab the Velox logger by the `velox` handle.

## `VeloxObject` Abstract Base Class
---

Functionality is exposed using the `VeloxObject` abstract base class (ABC). A subclass of a `velox.obj.VeloxObject` needs to implement three things in order for the library to know how to manage it.

* Your class must be defined with a `velox.obj.register_model` decorator around it.
* Your class must implement a `_save` object method that takes as input a file object and does whatever is needed to save the object.
* Your class must implement a `_load` class method (with the `@classmethod` decorator) that takes as input a file object and reconstructs and returns an instance of your class.

This allows you to abstract away much of the messiness in bookkeeping.

Here is a simple example showing all required components.

<!--begin_code-->
    #!python
    @register_model(
        registered_name='foobar',
        version='0.1.0-alpha',
        version_constraints='>=0.1.0,<0.2.0'
    )
    class ChurnModel(VeloxObject):
        def __init__(self, submodel):
            super(VeloxObject, self).__init__()
            self._submodel = submodel

        def _save(self, fileobject):
            pickle.dump(self, fileobject)

        @classmethod
        def _load(cls, fileobject):
            return pickle.load(fileobject)

        def predict(self, X):
            return self._submodel.predict(X)
<!--end_code-->


Here is a full example using [`gensim`](https://github.com/RaRe-Technologies/gensim) to build a topic model and keep track of all the necessary ETL-type objects that follow:

<!--begin_code-->  
    #!python

    from gensim.corpora import Dictionary
    from gensim.models.ldamulticore import LdaMulticore
    from spacy.en import English

    nlp = English()

    @register_model('lda', '0.2.1')
    class LDAModel(VeloxObject):

        def __init__(self, n_components=50):
            super(LDAModel, self).__init__()

            self._n_components = n_components

        @staticmethod
        def tokenize(text):
            return [t.text.lower() for t in nlp(unicode(text)) if t.text.strip()]

        def fit(self, texts, passes=5, n_workers=1, no_below=5, no_above=0.2):
            tokenized = map(self.tokenize, texts)

            self._dictionary = Dictionary(tokenized)
            self._dictionary.filter_extremes(no_below=no_below, no_above=no_above)

            self._lda = LdaMulticore(
                corpus=map(self._dictionary.doc2bow, tokenized),
                workers=n_workers,
                num_topics=self._n_components,
                id2word=self._dictionary,
                passes=passes
            )

            return self

        def transform(self, texts):
            feats = (self._dictionary.doc2bow(self.tokenize(q)) for q in texts)
            vecs = list(self._lda[feats])

            X = np.zeros((len(vecs), self._lda.num_topics))
            for i, v in enumerate(vecs):
                ix, val = zip(*v)
                X[i][np.array(ix)] = val
            return X

        def _save(self, fileobject):
            with tarfile.open(fileobject.name, 'w:') as tf:
                with tempfile.NamedTemporaryFile() as tmp:
                    self._dictionary.save(tmp.name)
                    tf.add(tmp.name, 'dict.model')

                tmpdir = tempfile.mkdtemp()

                sp = os.path.join(tmpdir, 'lda.model')
                self._lda.save(sp)

                tf.add(tmpdir, 'lda.dir')
            print tmpdir

        @classmethod
        def _load(cls, fileobject):

            tmpdir = tempfile.mkdtemp()
            with tarfile.open(fileobject.name, 'r:*') as tf:
                tf.extractall(tmpdir)

            model = cls()

            _dictionary = Dictionary.load(os.path.join(tmpdir, 'dict.model'))
            _lda = LdaMulticore.load(os.path.join(tmpdir, 'lda.dir', 'lda.model'))

            setattr(model, '_dictionary', _dictionary)
            setattr(model, '_lda', _lda)

            setattr(model, '_n_components', model._lda.num_topics)
            shutil.rmtree(tmpdir)

            return model
<!--end_code-->


"""
import logging

# supress "No handlers could be found for logger XXXXX" error
logging.getLogger('velox').addHandler(logging.NullHandler())

__version__ = '0.1.0'

from .obj import VeloxObject, register_model

import filesystem
import exceptions
import tools
import obj
import wrapper

__all__ = ['filesystem', 'exceptions', 'tools', 'obj', 'wrapper']

from pyshex.utils.schema_loader import SchemaLoader
from wikidataintegrator import wdi_core
import json
import pdb
import requests

"""
Authors:
  Andra Waagmeester (andra' at ' micelio.be)

This file is part of the WikidataIntegrator.

"""
__author__ = 'Andra Waagmeester'
__license__ = 'MIT'

# TODO: move this to class
INVALID_PROP = -1

class WikibaseEngine(object):

    def __init__(self, wikibase_url='', wikibase_sparql_endpoint=''):
        """
        constructor
        :param wikibase_url: The base url of the wikibase being accessed (e.g. for wikidata https://www.wikidata.org
        """
        self.wikibase_url = wikibase_url
        self.wikibase_api = wikibase_url + "/w/api.php"
        self.wikibase_sparql = wikibase_sparql_endpoint
        self.local_item_engine = wdi_core.WDItemEngine.wikibase_item_engine_factory(
            self.wikibase_api, self.wikibase_sparql)
        self.mappings_cache = {} # maps entity ids between source and target wibase

    @classmethod
    def extractProperties(cls, d, properties):
        for k, v in d.items():
            if isinstance(v, dict):
                cls.extractProperties(v, properties)
            elif isinstance(v, list):
                for vl in v:
                    if isinstance(vl, dict):
                        cls.extractProperties(vl, properties)
            else:
                if k == "predicate" and v != "label" and v != "description":
                    properties.append(v)

    def copyProperties(self, login, wikibase_source, source_schema, languages=["en", "nl"]):
        """
        Copy the properties from a wikibase instance to another using a ShEx schema.

        :param login: An object of type WDLogin, which holds the credentials of the target wikibase instance.
        :type login: wdi_login.WDLogin
        :param wikibase_source: Base URL pointing to the source wikibase instance where the properties are stored.
        :type wikibase_source: str
        :param source_schema: URL pointing to an entity schema from which the properties
            will be obtained (e.g. https://www.wikidata.org/wiki/Special:EntitySchemaText/E37).
        :type source_schema: str
        :param languages: List of languages the labels and descriptions of the properties in the target wikibase.
        :type languages: List[str]
        """
        loader = SchemaLoader()
        shex = requests.get(source_schema).text
        schema = loader.loads(shex)
        model = json.loads(schema._as_json_dumps())
        properties = []
        self.extractProperties(model, properties)
        props = list(set(properties))
        for prop in props:
            p = prop.split("/")
            if p[-1].startswith("P"):
                prop_id = p[-1]
                print(prop_id)
                page = json.loads(requests.get(wikibase_source + \
                                  "/w/api.php?action=wbgetentities&format=json&ids=" + prop_id).text)
                #pdb.set_trace()
                print(self.createProperty(login, page['entities'][prop_id],
                                          wikibase_source,
                                          languages))

    def createItem(self, login, wd_json, wikibase_source, languages, deep_copy=True):
        return self.createEntity(login, wd_json, wikibase_source, languages, deep_copy, etype='item')

    def createProperty(self, login, wd_json, wikibase_source, languages, deep_copy=True):
        property_datatype = wd_json['datatype']
        return self.createEntity(login, wd_json, wikibase_source, languages, deep_copy,
                                 etype='property', property_datatype=property_datatype)

    def createEntity(self, login, wd_json, wikibase_source, languages, deep_copy, etype, **kwargs):
        labels, descriptions, source_id = (wd_json['labels'], wd_json['descriptions'], wd_json['id'])

        # TODO: if we are making a deep copy we should skip this and add the statements regardless
        if self.existsEntity(source_id, wikibase_source, etype):
            target_id = self._get_target_id_of(source_id, wikibase_source, etype)
            print(f"{source_id} already exists -> {target_id}")
            return target_id

        item = self.local_item_engine(new_item=True)
        if 'en' not in languages:
            languages.append('en')

        for language in languages:
            if language in labels.keys():
                item.set_label(labels[language]["value"], lang=language)
            if language in descriptions.keys():
                item.set_description(descriptions[language]["value"], lang=language)

        try:
            new_item_id = item.write(login, entity_type=etype, **kwargs)
        except wdi_core.WDApiError as e:
            pdb.set_trace()
            if 'wikibase-api-not-recognized-datatype' in e.wd_error_msg['error']['messages'][0]['name']:
                self.mappings_cache[source_id] = INVALID_PROP
            return None

        self.mappings_cache[source_id] = new_item_id # update cache

        claims = wd_json['claims']
        if len(claims) == 0:
            # no statements to add, we can finish
            return new_item_id
        elif not deep_copy:
            # we don't want to add the statements
            wd_mapping = wdi_core.WDUrl(value="http://www.wikidata.org/entity/"+source_id, prop_nr="P1")
            item.update([wd_mapping], append_value=["P1"])
            item.write(login, entity_type=etype)
            return new_item_id

        item_data = []
        append_value = []
        for claim_id, claim_data in claims.items():
            append_value.append(claim_id)
            claim_prop_json = self._load_entity_json(wikibase_source, claim_id,
                                                     languages)['entities'][claim_id]
            prop_target_id = self.createProperty(login, claim_prop_json, wikibase_source,
                                                 languages, deep_copy=False)
            for data in claim_data:
                # qualifiers
                item_qualifiers = []
                if 'qualifiers' in data:
                    for qualifier_id, qualifier_data in data["qualifiers"].items():
                        qualifier_prop_json = self._load_entity_json(wikibase_source, qualifier_id,
                                                                     languages)['entities'][qualifier_id]
                        qualifier_target_id = self.createProperty(login, qualifier_prop_json,
                                                                  wikibase_source,
                                                                  languages, deep_copy=False)
                        item_qualifiers += [self._create_statement(login, wikibase_source,
                                                                   languages, value,
                                                                   qualifier_target_id,
                                                                   is_qualifier=True)
                                            for value in qualifier_data]
                    item_qualifiers = list(filter(None, item_qualifiers))

                # References
                item_references = []
                if 'references' in data:
                    for reference in data["references"]:
                        for reference_id, reference_data in reference["snaks"].items():
                            reference_prop_json = self._load_entity_json(wikibase_source, reference_id,
                                                                         languages)['entities'][reference_id]
                            reference_target_id = self.createProperty(login, reference_prop_json,
                                                                      wikibase_source,
                                                                      languages, deep_copy=False)
                            reference_statements = [self._create_statement(login, wikibase_source,
                                                                       languages, value,
                                                                       reference_target_id, is_reference=True)
                                                for value in reference_data]
                            reference_statements = list(filter(None, reference_statements))
                            item_references += [reference_statements]

                mainsnak = data["mainsnak"]
                statement = self._create_statement(login, wikibase_source, languages,
                                                mainsnak, prop_target_id,
                                                qualifiers=item_qualifiers,
                                                references=item_references)
                if statement is not None:
                    item_data.append(statement)

        item.update(item_data, append_value)
        item.write(login, entity_type=etype)
        return new_item_id


    def existsEntity(self, source_prop_id, wikibase_source, etype):
        return self._get_target_id_of(source_prop_id, wikibase_source, etype) is not None

    def existsProperty(self, source_prop_id, wikibase_source):
        return self._get_target_id_of(source_prop_id, wikibase_source, etype='property') is not None

    def existsItem(self, source_item_id, wikibase_source):
        return self._get_target_id_of(source_item_id, wikibase_source, etype='item') is not None

    def _create_statement(self, login, wikibase_source, languages, mainsnak, prop_id, **kwargs):
        if mainsnak['snaktype'] == 'novalue':
            # this is a temporal fix
            # TODO: handle novalues
            return None

        datatype = mainsnak['datatype']
        datavalue = mainsnak['datavalue']

        if datatype == 'wikibase-item':
            source_item_id = datavalue['value']['id']
            source_item_json = self._load_entity_json(wikibase_source, source_item_id,
                                                      languages)['entities'][source_item_id]
            target_item_id = self.createItem(login, source_item_json, wikibase_source,
                                             languages, deep_copy=False)
            return wdi_core.WDItemID(value=target_item_id, prop_nr=prop_id, **kwargs)
        elif datatype == 'wikibase-property':
            source_prop_id = datavalue['value']['id']
            source_prop_json = self._load_entity_json(wikibase_source, source_prop_id,
                                                      languages)['entities'][source_prop_id]
            target_prop_id = self.createProperty(login, source_prop_json, wikibase_source,
                                                 languages, deep_copy=False)
            return wdi_core.WDProperty(value=target_prop_id, prop_nr=prop_id, **kwargs)
        elif datatype == "time":
            return wdi_core.WDTime(time=datavalue["value"]["time"], prop_nr=prop_id,
                                   precision=datavalue["value"]["precision"],
                                   timezone=datavalue["value"]["timezone"], **kwargs)
        elif datatype == "monolingualtext":
            return wdi_core.WDMonolingualText(value=datavalue["value"]["text"], prop_nr=prop_id,
                                              language=datavalue["value"]["language"], **kwargs)
        elif datatype == "external-id":
            return wdi_core.WDExternalID(value=datavalue["value"], prop_nr=prop_id, **kwargs)
        elif datatype == "":
            return wdi_core.WDGlobeCoordinate(latitude=datavalue["latitude"],
                                              longitude=datavalue["longitude"],
                                              precision=datavalue["precision"],
                                              prop_nr=prop_id, **kwargs)
        elif datatype == "string":
            return wdi_core.WDString(value=datavalue["value"], prop_nr=prop_id, **kwargs)
        elif datatype == "url":
            return wdi_core.WDUrl(value=datavalue["value"], prop_nr=prop_id, **kwargs)

        else:
            print(f"Not recognized datatype '{datatype}'. Returning None...")
            return None # TODO extend to other types


    def _get_target_id_of(self, source_item_id, wikibase_source, etype, language='en'):
        if source_item_id in self.mappings_cache:
            return self.mappings_cache[source_item_id]
        else:
            return None

        # source_item_json = json.loads(
        #     requests.get(f"{wikibase_source}/w/api.php?action=wbgetentities&format=json&ids={source_item_id}" + \
        #                  "&props=labels&languages=en").text)['entities'][source_item_id]
        # if language not in source_item_json['labels']:
        #     # logger.info("Can't copy entity %s to target wikibase, since it has no label for language %s ", source_item_id, language)
        #     self.mappings_cache[source_item_id] = INVALID_PROP
        #     return self._get_target_id_of(source_item_id, wikibase_source, etype, language)
        #
        # source_item_label = source_item_json['labels'][language]['value']
        # items = json.loads(
        #     requests.get(f"{self.wikibase_api}?action=wbsearchentities&format=json&search={source_item_label}" + \
        #                  f"&language={language}&type={etype}").text
        # )
        #
        # if "search" not in items or len(items["search"]) == 0:
        #     self.mappings_cache[source_item_id] = None
        # else:
        #     found = False
        #     for search_result in items["search"]:
        #         if search_result["label"] == source_item_label:
        #             self.mappings_cache[source_item_id] = items["search"][0]["id"]
        #     if not found:
        #         self.mappings_cache[source_item_id] = None
        # return self._get_target_id_of(source_item_id, wikibase_source, etype, language)

        # TODO: implement this
        pdb.set_trace()
        query = f"PREFIX wdt: <{self.wikibase_url}prop/direct/>"
        query += """
            SELECT ?item ?itemLabel ?wikidata_mapping WHERE {
                ?item wdt:P1 ?wikidata_mapping .
                SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
            }
        """
        for result in wdi_core.WDItemEngine.execute_sparql_query(query, endpoint=self.wikibase_sparql)["results"]["bindings"]:
            wikidata_id = result["wikidata_mapping"]["value"].replace("http://www.wikidata.org/entity", "")
            self.mappings_cache[wikidata_id] = result["item"]["value"].replace(self.wikibase_url+"entity/", "")

        return self._get_target_id_of(source_item_id, wikibase_source, etype, language)


    def getNamespace(self, nsName):
        namespaces = json.loads(
            requests.get(self.wikibase_api + "?action=query&format=json&meta=siteinfo&formatversion=2&siprop=namespaces").text)
        for namespace in namespaces["query"]["namespaces"].keys():
            if namespaces["query"]["namespaces"][namespace]["name"] == nsName:
                return namespace

    def listProperties(self):
        """
        List the properties of the target wikibase instance.

        :returns: List of labels of each property in the wikibase.
        :rtype: List[str]
        """
        property_labels = []
        ns = self.getNamespace("Property")
        query_url = self._build_list_properties_query(ns)
        properties = json.loads(requests.get(query_url).text)
        if 'query' not in properties:
            # wikibase is empty
            return []

        self._extract_labels_from_properties(properties, property_labels)
        while 'continue' in properties:
            gapcontinue = properties['continue']['gapcontinue']
            query_url = self._build_list_properties_query(ns, gapcontinue)
            properties = json.loads(requests.get(query_url).text)
            self._extract_labels_from_properties(properties, property_labels)

        return property_labels

    def _build_list_properties_query(self, ns, gapcontinue=None):
        res = [self.wikibase_api,
               "?action=query&format=json&prop=pageterms&generator=allpages&wbptterms" \
               "=label&gapnamespace=",
               ns]
        if gapcontinue is not None:
            res.append("&gapcontinue=")
            res.append(gapcontinue)
        return ''.join(res)

    def _extract_labels_from_properties(self, properties, propertyLabels):
        for prop in properties["query"]["pages"].values():
            for label in prop["terms"]["label"]:
                propertyLabels.append(label)

    def _load_entity_json(self, wikibase, entity_ids, languages):
        return json.loads(requests.get(f"{wikibase}/w/api.php?action=wbgetentities&ids={entity_ids}" + \
            f"&format=json&languages={'|'.join(languages)}").text)

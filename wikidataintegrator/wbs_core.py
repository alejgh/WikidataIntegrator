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

def visit_shape_expression(source_schema, callback_map):
    def _visit_shape_expression_node(k, v, context):
        if k in callback_map:
            callback_map[k](v, context)

        if isinstance(v, dict):
            for new_k, new_v in v.items():
                _visit_shape_expression_node(new_k, new_v, context)
        elif isinstance(v, list):
            for child in v:
                if isinstance(child, dict):
                    for new_k, new_v in child.items():
                        _visit_shape_expression_node(new_k, new_v, context)

    context = {}
    for k, v in source_schema.items():
        _visit_shape_expression_node(k, v, context)
    return context

class WikibaseEngine(object):

    def __init__(self, wbsource_url, wbsource_sparql_endpoint, target_login,
                 wbtarget_url, wbtarget_sparql_endpoint):
        """
        constructor
        :param wikibase_url: The base url of the wikibase being accessed (e.g. for wikidata https://www.wikidata.org
        """
        self.wbsource_url = wbsource_url
        self.wbsource_sparql_endpoint = wbsource_sparql_endpoint
        self.wbtarget_url = wbtarget_url
        self.wbtarget_api = wbtarget_url + "/w/api.php"
        self.wbtarget_sparql = wbtarget_sparql_endpoint
        self.login = target_login
        self.local_item_engine = wdi_core.WDItemEngine.wikibase_item_engine_factory(
            self.wbtarget_api, self.wbtarget_sparql)
        self.mappings_cache = {} # maps entity ids between source and target wibase

    @classmethod
    def extractPropertiesFrom(cls, schema_json):
        properties = []

        def on_predicate(value, context):
            if "label" not in value and "description" not in value:
                # extract "PXX" from complete url
                property_tokens = value.split("/")
                if property_tokens[-1].startswith("P"):
                    properties.append(property_tokens[-1])

        callback_map = {
            "predicate": on_predicate
        }
        visit_shape_expression(schema_json, callback_map)
        return list(set(properties))

    @classmethod
    def extractItemsFrom(cls, schema_json):
        items = []
        def on_values(values, context):
            for val in values:
                item_tokens = val.split('/')
                if item_tokens[-1].startswith('Q'):
                    items.append(item_tokens[-1])

        callback_map = {
            "values": on_values
        }
        visit_shape_expression(schema_json, callback_map)
        return list(set(items))

    def create_subset_from(self, source_schema, sparql_query, languages=["en", "nl"]):
        """

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
        model_json = self._load_schema(source_schema)

        self.copyItems(model_json)
        self.copyProperties(model_json)

        pdb.set_trace()
        # obtain nodes from query -> nodes
        nodes = []
        for result in wdi_core.WDItemEngine.execute_sparql_query(sparql_query, endpoint=self.wbsource_sparql_endpoint)["results"]["bindings"]:
            nodes.append(result["item"]["value"].replace("http://www.wikidata.org/entity/", ""))


        # TODO: validate nodes against given schema -> validated nodes
        #validated_nodes = [node for node in nodes if node._conforms_to_shape(source_schema)]

        # copy each validated node to the target wb, writting just the properties and values that appear in the ShEx
        pass

    def copyItems(self, schema_json, languages=["en", "nl"]):
        items = self.extractItemsFrom(schema_json)
        for item_id in items:
            print(item_id)
            page = json.loads(requests.get(self.wbsource_url + \
                              "/w/api.php?action=wbgetentities&format=json&ids=" + item_id).text)
            target_id = self.createItem(page['entities'][item_id], languages)
            print(f"Target ID -> {target_id}")


    def copyProperties(self, schema_json, languages=["en", "nl"]):
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
        properties = self.extractPropertiesFrom(schema_json)
        for prop_id in properties:
            print(prop_id)
            page = json.loads(requests.get(self.wbsource_url + \
                              "/w/api.php?action=wbgetentities&format=json&ids=" + prop_id).text)
            target_id = self.createProperty(page['entities'][prop_id], languages)
            print(f"Target ID -> {target_id}")

    def createItem(self, wd_json, languages, deep_copy=True):
        """ Creates an item in the target wikibase """
        return self.createEntity(wd_json, languages, deep_copy, etype='item')

    def createProperty(self, wd_json, languages, deep_copy=True):
        property_datatype = wd_json['datatype']
        return self.createEntity(wd_json, languages, deep_copy,
                                 etype='property', property_datatype=property_datatype)

    def createEntity(self, wd_json, languages, deep_copy, etype, **kwargs):
        labels, descriptions, source_id = (wd_json['labels'], wd_json['descriptions'], wd_json['id'])

        if self.existsEntity(source_id, etype):
            target_id = self._get_target_id_of(source_id, etype)
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
            new_item_id = item.write(self.login, entity_type=etype, **kwargs)
        except wdi_core.WDApiError as e:
            if 'wikibase-api-not-recognized-datatype' in e.wd_error_msg['error']['messages'][0]['name']:
                self.mappings_cache[source_id] = INVALID_PROP
            return INVALID_PROP

        self.mappings_cache[source_id] = new_item_id # update mappings

        claims = wd_json['claims']
        if len(claims) == 0:
            # no statements to add, we can finish
            return new_item_id
        elif not deep_copy:
            # we don't want to add the statements
            wd_mapping = wdi_core.WDUrl(value="http://www.wikidata.org/entity/"+source_id, prop_nr="P1")
            item.update([wd_mapping], append_value=["P1"])
            item.write(self.login, entity_type=etype)
            return new_item_id

        self._add_statements_to_item(item, etype, claims, languages)
        return new_item_id


    def existsEntity(self, source_prop_id, etype):
        return self._get_target_id_of(source_prop_id, etype) is not None

    def existsProperty(self, source_prop_id):
        return self._get_target_id_of(source_prop_id, etype='property') is not None

    def existsItem(self, source_item_id):
        return self._get_target_id_of(source_item_id, etype='item') is not None

    def _add_statements_to_item(self, item, etype, claims, languages):
        item_data = []
        append_value = []
        for claim_id, claim_data in claims.items():
            append_value.append(claim_id)
            claim_prop_json = self._load_entity_json(self.wbsource_url, claim_id,
                                                     languages)['entities'][claim_id]
            prop_target_id = self.createProperty(claim_prop_json,
                                                 languages, deep_copy=False)
            if prop_target_id == INVALID_PROP:
                # invalid property (datatype not supported), skip
                continue

            for data in claim_data:
                # qualifiers
                item_qualifiers = self._get_qualifiers_from(data, languages)

                # References
                item_references = self._get_references_from(data, languages)

                mainsnak = data["mainsnak"]
                statement = self._create_statement(languages,
                                                   mainsnak, prop_target_id,
                                                   qualifiers=item_qualifiers,
                                                   references=item_references)
                if statement is not None:
                    item_data.append(statement)

        item.update(item_data, append_value)
        item.write(self.login, entity_type=etype)

    def _create_statement(self, languages, mainsnak, prop_id, **kwargs):
        if mainsnak['snaktype'] == 'novalue':
            # this is a temporal fix
            # TODO: handle novalues
            return None

        datatype = mainsnak['datatype']
        datavalue = mainsnak['datavalue']

        if datatype == 'wikibase-item':
            source_item_id = datavalue['value']['id']
            source_item_json = self._load_entity_json(self.wbsource_url, source_item_id,
                                                      languages)['entities'][source_item_id]
            target_item_id = self.createItem(source_item_json,
                                             languages, deep_copy=False)
            return wdi_core.WDItemID(value=target_item_id, prop_nr=prop_id, **kwargs)
        elif datatype == 'wikibase-property':
            source_prop_id = datavalue['value']['id']
            source_prop_json = self._load_entity_json(self.wbsource_url, source_prop_id,
                                                      languages)['entities'][source_prop_id]
            target_prop_id = self.createProperty(source_prop_json,
                                                 languages, deep_copy=False)
            if target_prop_id == INVALID_PROP:
                return None
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
        elif datatype == "globe-coordinate":
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

    def _get_references_from(self, data, languages):
        item_references = []
        if 'references' not in data:
            return item_references

        for reference in data["references"]:
            for reference_id, reference_data in reference["snaks"].items():
                reference_prop_json = self._load_entity_json(self.wbsource_url, reference_id,
                                                             languages)['entities'][reference_id]
                reference_target_id = self.createProperty(reference_prop_json,
                                                          languages, deep_copy=False)
                if reference_target_id == INVALID_PROP:
                    continue
                reference_statements = [self._create_statement(languages, value,
                                                           reference_target_id, is_reference=True)
                                        for value in reference_data]
                reference_statements = list(filter(None, reference_statements))
                item_references += [reference_statements]
        return item_references


    def _get_qualifiers_from(self, data, languages):
        item_qualifiers = []
        if 'qualifiers' not in data:
            return item_qualifiers

        for qualifier_id, qualifier_data in data["qualifiers"].items():
            qualifier_prop_json = self._load_entity_json(self.wbsource_url, qualifier_id,
                                                         languages)['entities'][qualifier_id]
            qualifier_target_id = self.createProperty(qualifier_prop_json,
                                                      languages, deep_copy=False)
            if qualifier_target_id == INVALID_PROP:
                continue
            item_qualifiers += [self._create_statement(languages, value,
                                                       qualifier_target_id,
                                                       is_qualifier=True)
                                for value in qualifier_data]
        item_qualifiers = list(filter(None, item_qualifiers))
        return item_qualifiers

    def _get_target_id_of(self, source_item_id, etype, language='en'):
        if source_item_id in self.mappings_cache:
            return self.mappings_cache[source_item_id]
        else:
            return None

        # TODO: implement this
        query = f"PREFIX wdt: <{self.wbtarget_url}prop/direct/>"
        query += """
            SELECT ?item ?itemLabel ?wikidata_mapping WHERE {
                ?item wdt:P1 ?wikidata_mapping .
                SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
            }
        """
        for result in wdi_core.WDItemEngine.execute_sparql_query(query, endpoint=self.wbtarget_sparql)["results"]["bindings"]:
            wikidata_id = result["wikidata_mapping"]["value"].replace("http://www.wikidata.org/entity", "")
            self.mappings_cache[wikidata_id] = result["item"]["value"].replace(self.wbtarget_url+"entity/", "")

        return self._get_target_id_of(source_item_id, etype, language)


    def getNamespace(self, nsName):
        namespaces = json.loads(
            requests.get(self.wbtarget_api + "?action=query&format=json&meta=siteinfo&formatversion=2&siprop=namespaces").text)
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
        res = [self.wbtarget_api,
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

    def _load_schema(self, schema_url):
        loader = SchemaLoader()
        shex = requests.get(schema_url).text
        schema = loader.loads(shex)
        return json.loads(schema._as_json_dumps())

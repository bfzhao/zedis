package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class SearchHandler extends RedisCommandHandler {
    SearchHandler(InMemorySharedStore sharedStore) {
        super("Search", sharedStore);

        commands.put("FT.AGGREGATE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.AGGREGATE index query   [VERBATIM]   [LOAD count field [field ...]]   [TIMEOUT timeout]   [ GROUPBY nargs property [property ...] [ REDUCE function nargs arg [arg ...] [AS name] [ REDUCE function nargs arg [arg ...] [AS name] ...]] ...]]   [ SORTBY nargs [ property ASC | DESC [ property ASC | DESC ...]] [MAX num] [WITHCOUNT]   [ APPLY expression AS name [ APPLY expression AS name ...]]   [ LIMIT offset num]   [FILTER filter]   [ WITHCURSOR [COUNT read_size] [MAXIDLE idle_time]]   [ PARAMS nargs name value [ name value ...]]   [DIALECT dialect]",
                        "Run a search query on an index and perform aggregate transformations on the results"
                )
        );
        commands.put("FT.ALIASADD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.ALIASADD alias index",
                        "Adds an alias to the index"
                )
        );
        commands.put("FT.ALIASDEL",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.ALIASDEL alias",
                        "Deletes an alias from the index"
                )
        );
        commands.put("FT.ALIASUPDATE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.ALIASUPDATE alias index",
                        "Adds or updates an alias to the index"
                )
        );
        commands.put("FT.ALTER",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.ALTER {index} [SKIPINITIALSCAN] SCHEMA ADD {attribute} {options} ...",
                        "Adds a new field to the index"
                )
        );
        commands.put("FT.CONFIG GET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.CONFIG GET option",
                        "Retrieves runtime configuration options"
                )
        );
        commands.put("FT.CONFIG SET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.CONFIG SET option value",
                        "Sets runtime configuration options"
                )
        );
        commands.put("FT.CREATE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.CREATE index   [ON HASH | JSON]   [PREFIX count prefix [prefix ...]]   [FILTER {filter}]  [LANGUAGE default_lang]   [LANGUAGE_FIELD lang_attribute]   [SCORE default_score]   [SCORE_FIELD score_attribute]   [PAYLOAD_FIELD payload_attribute]   [MAXTEXTFIELDS]   [TEMPORARY seconds]   [NOOFFSETS]   [NOHL]   [NOFIELDS]   [NOFREQS]   [STOPWORDS count [stopword ...]]   [SKIPINITIALSCAN]  SCHEMA field_name [AS alias] TEXT | TAG | NUMERIC | GEO | VECTOR | GEOSHAPE [ SORTABLE [UNF]]   [NOINDEX] [ field_name [AS alias] TEXT | TAG | NUMERIC | GEO | VECTOR | GEOSHAPE [ SORTABLE [UNF]] [NOINDEX] ...]",
                        "Creates an index with the given spec"
                )
        );
        commands.put("FT.CURSOR DEL",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.CURSOR DEL index cursor_id",
                        "Deletes a cursor"
                )
        );
        commands.put("FT.CURSOR READ",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.CURSOR READ index cursor_id [COUNT read_size]",
                        "Reads from a cursor"
                )
        );
        commands.put("FT.DICTADD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.DICTADD dict term [term ...]",
                        "Adds terms to a dictionary"
                )
        );
        commands.put("FT.DICTDEL",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.DICTDEL dict term [term ...]",
                        "Deletes terms from a dictionary"
                )
        );
        commands.put("FT.DICTDUMP",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.DICTDUMP dict",
                        "Dumps all terms in the given dictionary"
                )
        );
        commands.put("FT.DROPINDEX",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.DROPINDEX index   [DD]",
                        "Deletes the index"
                )
        );
        commands.put("FT.EXPLAIN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.EXPLAIN index query   [DIALECT dialect]",
                        "Returns the execution plan for a complex query"
                )
        );
        commands.put("FT.EXPLAINCLI",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.EXPLAINCLI index query   [DIALECT dialect]",
                        "Returns the execution plan for a complex query"
                )
        );
        commands.put("FT.INFO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.INFO index",
                        "Returns information and statistics on the index"
                )
        );
        commands.put("FT.PROFILE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.PROFILE index SEARCH | AGGREGATE [LIMITED] QUERY query",
                        "Performs a `FT.SEARCH` or `FT.AGGREGATE` command and collects performance information"
                )
        );
        commands.put("FT.SEARCH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.SEARCH index query   [NOCONTENT]   [VERBATIM] [NOSTOPWORDS]   [WITHSCORES]   [WITHPAYLOADS]   [WITHSORTKEYS]   [FILTER numeric_field min max [ FILTER numeric_field min max ...]]   [GEOFILTER geo_field lon lat radius m | km | mi | ft [ GEOFILTER geo_field lon lat radius m | km | mi | ft ...]]   [INKEYS count key [key ...]] [ INFIELDS count field [field ...]]   [RETURN count identifier [AS property] [ identifier [AS property] ...]]   [SUMMARIZE [ FIELDS count field [field ...]] [FRAGS num] [LEN fragsize] [SEPARATOR separator]]   [HIGHLIGHT [ FIELDS count field [field ...]] [ TAGS open close]]   [SLOP slop]   [TIMEOUT timeout]   [INORDER]   [LANGUAGE language]   [EXPANDER expander]   [SCORER scorer]   [EXPLAINSCORE]   [PAYLOAD payload]   [SORTBY sortby [ ASC | DESC] [WITHCOUNT]]   [LIMIT offset num]   [PARAMS nargs name value [ name value ...]]   [DIALECT dialect]",
                        "Searches the index with a textual query, returning either documents or just ids"
                )
        );
        commands.put("FT.SPELLCHECK",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.SPELLCHECK index query   [DISTANCE distance]   [TERMS INCLUDE | EXCLUDE dictionary [terms [terms ...]]]   [DIALECT dialect]",
                        "Performs spelling correction on a query, returning suggestions for misspelled terms"
                )
        );
        commands.put("FT.SYNDUMP",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.SYNDUMP index",
                        "Dumps the contents of a synonym group"
                )
        );
        commands.put("FT.SYNUPDATE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.SYNUPDATE index synonym_group_id   [SKIPINITIALSCAN] term [term ...]",
                        "Creates or updates a synonym group with additional terms"
                )
        );
        commands.put("FT.TAGVALS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.TAGVALS index field_name",
                        "Returns the distinct tags indexed in a Tag field"
                )
        );
        commands.put("FT._LIST",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT._LIST",
                        "Returns a list of all existing indexes"
                )
        );
    }
}

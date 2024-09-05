package org.my;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@lombok.Setter
public class Argument {
    // mandatory
    private Map<String, String> namedValues = new HashMap<>();
    private Map<Integer, String> fixedTerms = new HashMap<>();
    private String[] listValues;

    // optional
    private Map<String, String[]> namedOptions = new HashMap<>();
    private String anonymousOptionValue;
    private Set<String> anonymousTerms = new HashSet<>();

    //
    void addNamedValue(String name, String value) {
        namedValues.put(name, value);
    }

    void addNamedOption(String name, String[] values) {
        namedOptions.put(name, values);
    }

    void addFixedTerm(int idx, String term) {
        fixedTerms.put(idx, term);
    }

    void addAnonymousTerm(String term) {
        anonymousTerms.add(term);
    }

    // operations
    public String valueWithName(String name) { return namedValues.get(name); }
    public String[] valueListDefault() { return listValues; }
    public String termAtPos(int i) { return fixedTerms.get(i); }

    public String[] optionWithName(String name) {  return namedOptions.get(name); }
    public boolean hasOption(String name) { return namedOptions.containsKey(name); }
    public String optionValueAnonymous() { return anonymousOptionValue; }
    public Set<String> optionValueTerms() { return anonymousTerms; }
}

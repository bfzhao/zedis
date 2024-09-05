package org.my;

import org.my.zedis.RespType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

@lombok.Getter
public class Command {
    private final BiFunction<Context, Argument, RespType> func;
    private final String syntax;
    private final String explain;
    private final Part[] parts;

    @lombok.Getter
    public static class Part {
        enum Type { Value, TermValue, ListValue, OptionAnonymous, OptionWithValue, OptionWithVarList, OptionWithTerms, OptionChoice}
        Type type;
        String showName;
        boolean optional;
        Integer cardinality;

        // simple value
        String valueName;

        // simple terms value
        String[] termsValueName;
        int termPosition;

        // List value
        String lengthRefer;

        // Option
        String optionName;
        int optionValueSize;
        Set<String> optionTermSet = new HashSet<>();

        // Choice option
        Part[] choiceOption;

        Part(Type t, String showName, boolean optional, Integer cardinality) {
            this.type = t;
            this.showName = showName;
            this.optional = optional;
            this.cardinality = cardinality;
        }

        Part(Type t, String showName, boolean optional) {
            this(t, showName, optional, null);
        }

        // Command: name
        public static Part ofValue(String showName) {
            Part p = new Part(Type.Value, showName, false);
            p.valueName = showName;
            return p;
        }

        // Command: <A | B>
        public static Part ofTermValue(int pos, String... terms) {
            Arrays.stream(terms).forEach(t -> {assert t.equals(t.toUpperCase());});
            Part p = new Part(Type.TermValue, buildShowName(Arrays.stream(terms), " | "), false);

            p.termPosition = pos;
            p.termsValueName = terms;
            return p;
        }

        // Command: a [a ...]
        // Command: a b [a b ...]
        public static Part ofListValue(String ...list) {
            Part p = new Part(Type.ListValue, buildShowName(Arrays.stream(list), " "), false, list.length);
            p.lengthRefer = null;
            return p;
        }

        // Command: length a [a ...]
        public static Part ofFixedLengthListValue(String length, String name) {
            Part p = new Part(Type.ListValue, name, false, 1);
            p.lengthRefer = length;
            return p;
        }

        // Command: [count]
        public static Part ofOptionAnonymous(String showName) {
            Part p = new Part(Type.OptionAnonymous, showName, true);
            p.optionName = null;
            return p;
        }

        // Command: [count [X]]
        public static Part ofOptionAnonymousWithTerm(String showName, String term) {
            assert (term.equals(term.toUpperCase()));
            Part p = new Part(Type.OptionAnonymous, showName, true);

            p.optionName = null;
            p.optionTermSet.add(term);
            return p;
        }

        // Command: [X]
        // Command: [X a]
        // Command: [X a b]
        public static Part ofOptionNamedSimple(String name, String ...optionValues) {
            assert (name.equals(name.toUpperCase()));
            Part p = new Part(Type.OptionWithValue, name, true);

            p.optionName = name;
            p.optionValueSize = optionValues.length;
            return p;
        }

        // Command [X a [a ...]]
        // Command [X a b [a b ...]]
        public static Part ofOptionNamedVarList(String name, String ...optionValues) {
            assert (name.equals(name.toUpperCase()));
            Part p = new Part(Type.OptionWithVarList, name, true, optionValues.length);

            p.optionName = name;
            p.optionValueSize = -1;
            return p;
        }

        // Command: [X <A | B | C >]
        public static Part ofOptionNamedTerms(String name, String ...terms) {
            assert (name.equals(name.toUpperCase()));
            Arrays.stream(terms).forEach(t -> {assert t.equals(t.toUpperCase());});
            Part p = new Part(Type.OptionWithTerms, name, true, 1);

            p.optionName = name;
            p.optionValueSize = 0;
            p.optionTermSet = new HashSet<>(Arrays.asList(terms));
            return p;
        }

        // Command: [NX | XX]
        public static Part ofOptionChoice(Part ...choiceOption) {
            Part p = new Part(Type.OptionChoice, buildShowName(Arrays.stream(choiceOption).map(x -> x.showName), " | "), true);

            p.choiceOption = choiceOption;
            return p;
        }

        private static String buildShowName(Stream<String> stream, String sep) {
            return stream.reduce("", (x, y) -> x += x.length() == 0? y: sep + y);
        }
    }

    public Command(BiFunction<Context, Argument, RespType> func, String syntax, String explain, Part... parts) {
        this.func = func;
        this.syntax = syntax;
        this.explain = explain;
        this.parts = parts;
    }

    public Command(BiFunction<Context, Argument, RespType> func, String syntax, String explain) {
        this(func, syntax, explain, (Part[]) null);
    }

    private int parseOption(RespType[] args, int idx, Argument argument, Part... parts) {
        int start = idx;
        String optName = args[idx++].asString();
        Part matched = null;
        for (Part co: parts) {
            if (co.getOptionName() == null || optName.compareToIgnoreCase(co.getOptionName()) == 0) {
                matched = co;
                break;
            }
        }

        if (matched == null) {
            idx--;
        } else {
            String[] optValue = null;
            switch (matched.getType()) {
                case OptionAnonymous: {
                    argument.setAnonymousOptionValue(optName);
                    if (matched.getOptionTermSet() != null && idx < args.length) {
                        String term = args[idx++].asString();
                        if (matched.optionTermSet.contains(term.toUpperCase()))
                            argument.addAnonymousTerm(term.toUpperCase());
                        else
                            throw new IllegalArgumentException("bad command, unknown term: " + term);
                    }
                    break;
                }
                case OptionWithValue: {
                    optValue = new String[matched.optionValueSize];
                    for (int i = 0; i < matched.optionValueSize; i++) {
                        if (idx >= args.length)
                            throw new IllegalArgumentException("bad command, out of range");
                        optValue[i] = args[idx++].asString();
                    }
                    argument.addNamedOption(optName.toUpperCase(), optValue);
                    break;
                }
                case OptionWithTerms: {
                    optValue = new String[matched.optionValueSize];
                    String term = args[idx++].asString();
                    if (!matched.optionTermSet.contains(term.toUpperCase()))
                        optValue[0] = term;
                    else
                        throw new IllegalArgumentException("bad command, unknown term: " + term);
                    argument.addNamedOption(optName.toUpperCase(), optValue);
                    break;
                }
                case OptionWithVarList: {
                    // FIXME: Redis uses a wired syntax make parsing a little not straight and it may be cause issue
                    argument.addNamedOption(optName.toUpperCase(), optValue);
                    break;
                }
                default:
            }
        }

        return idx - start;
    }

    private int parseTerms(RespType[] args, int idx, Argument argument, String[] terms, int position) {
        String name = args[idx].asString();
        boolean matched = false;
        for (String t: terms) {
            if (t.compareToIgnoreCase(name) == 0) {
                matched = true;
                argument.addFixedTerm(position, t);
                break;
            }
        }

        if (matched) {
            return 1;
        } else
            throw new IllegalArgumentException("bad term");
    }

    public Argument parseArguments(RespType[] args) {
        Argument argument = new Argument();
        if (parts != null) {
            int idx = 0;
            for (Part part: parts) {
                if (idx >= args.length) {
                    if (!part.optional)
                        throw new IllegalArgumentException("incomplete command: "+ part.showName + " missed");
                    else
                        break;
                }

                switch (part.getType()) {
                    case Value: {
                        argument.addNamedValue(part.getValueName(), args[idx++].asString());
                        break;
                    }
                    case OptionAnonymous:
                    case OptionWithValue:
                    case OptionWithVarList:
                    case OptionWithTerms: {
                        idx += parseOption(args, idx, argument, part);
                        break;
                    }
                    case OptionChoice: {
                        idx += parseOption(args, idx, argument, part.getChoiceOption());
                        break;
                    }
                    case ListValue: {
                        int n;
                        if (part.lengthRefer == null) {
                            n = args.length - idx;
                        } else {
                            n = Integer.parseInt(args[idx++].asString());
                        }
                        String[] value = new String[n];
                        for (int i = 0; i < n; i++) {
                            value[i] = args[idx++].asString();
                        }
                        argument.setListValues(value);
                        break;
                    }
                    case TermValue:
                        idx += parseTerms(args, idx, argument, part.getTermsValueName(), part.getTermPosition());
                        break;
                    default:
                        break;
                }
            }
        }

        return argument;
    }
}

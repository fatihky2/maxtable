# Introduction #

The following Coding Standards are meant as a guideline to developing, modifying and reviewing code in this Maxtable product. The goal in creating these Standards has been to provide an easy-to-use, realistic, useful document to aid Maxtable engineers in producing high quality, high performing, low maintenance Maxtable code. Not every aspect of coding is addressed here, nor are items included in the document meant to be absolute hard and fast rules.

The best coding guideline is the existing Maxtable code. New or modified code should not stand out from the existing code. If a coding aspect is not addressed here, take some time to find existing examples of a similar situation, and adhere to the format/style of that example. Alternatively, when a conflict exists between existing code and this standard, the guidelines presented here should take precedence.


# Coding Style & Formats #

1. Comment the obscure not the obvious, the why, not the what. Also, when commenting your code, ensure that the comments are going to be useful.

2. Single line comments should be placed on line separate from code, except for variable, parameter, or data structure declarations. Align the comments with the code.
```
        Preferred:

        /* Building the response information. */
        col_buf = MEMALLOCHEAP(rlen);

        Avoid:

        col_buf = MEMALLOCHEAP(rlen);   /* Building the response information. */
```

3. Multi-line comments should have the delimiting strings '/**' and '**/' on separate lines. Each comment line begins with '' followed by a blank. For example:
```
        /*
        ** Fill search information for the searching in the
        ** block.
        */
        tabinfo->t_sinfo->sicolval = col_val;
        tabinfo->t_sinfo->sicollen = col_len;
```

4. Comment each variable, structure, and macro declaration. Comment each entry in a structure.

5. Indent each variable declaration in a function by one tab stop.

6. Tab stops should be set at every 8 characters.

7. All variable declarations should be on separate lines.

8. Line braces up vertically on lines by themselves. For example:
```
        if (....
        {
                ...
        }

        while(...
        {
                ...
        }
```
9. Line braces up under the first letter of switch, if, while, for, struct, union, and do statements.

10. The else statement should line up below the if statement.

11. Switch statements should have the following format. Comment the instances where one branch falls through to the next, and always provide a default branch.
```
        switch (...
        {
            case ...:
            case ...:
            ....
            default:
                ....;
        }
```

12. Always use braces with if, while, for, and do statements, even when the body contains just one statement.

13. If while or for statements are not followed by statements to be executed in the loop, use the continue statement.

14. Variable names can be a combination of upper and lower case letters, digits, and underscores, and should be limited to 31 or fewer characters.
Global variables begin with an upper case character, but include at least one lower case letter to avoid confusion with macro names.
Local variables should begin with a lower case character.

15. If a macro is declared with parameters, surround each instance of the macro parameter with parentheses.

16. Never hard code a value (with the exception of 0 and 1); instead use #define in a related include file.
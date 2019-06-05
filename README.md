### Exercise 1: Windows Functions

### Window Functions
 
Window functions significantly improve the expressiveness power of SparkR. Tipically, builtin functions like `round` or `abs` take values from a single row as input and generate a single return for every input row.
Other functions can be seen as *aggregate functions*, e.g. `sum`, `maximum` or `average`. These operate not only on a signgle row but on a group of rows and calculate a **single return value for every group**.

While these are extremely useful in practice, there is still a wide range of operations that cannot be expressed just by using them.
More specifically, there isn't a way to mix both approaches, i.e. operate on a group of rows while still returning a **single value for every input row**. This can be a hige limitation for certain type of analysis. The following are simple examples of calcuations that require this capability:

    - Moving average

    - Cumulative sums

    - Generally, every calculation that needs to access other row before the current one being processed.

Window Functions fill this gap.

These are functions that will return a value for every input row of a table. The value that is returned, however, is calculated based on a group of rows, which tipically is called a *Frame*.
Note that there is nothing preventing each row of having a unique frame associated with it. This last point is what makes Window Functions extremely useful.

 
There are various functions available:

**Ranking Functions**

`row_number`: assigns unique numbers to each row within the Frame given by the *order by* clause

`rank`: behaves like `row_number` except that "equal" rows are ranked the same

`dense_rank`: behaves like a `rank` with no gaps

### Exercise

Consider the `products` table. Answer the following questions:

    1. What is the best selling product in each category?
    2. What are the best and second best-selling product in each category?
    3. What is the difference between the revenue of each product and the best selling product in the same category of the product?
    4. What is the difference between the revenue of each product and the average revenue of the category if that product?
    
    
    


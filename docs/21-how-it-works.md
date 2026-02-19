# How It Works

A simple explanation of the identity resolution process.

## The "Business Card" Analogy

Imagine a box of mixed-up business cards.
*   Card A: "John Smith", `john@gmail.com`
*   Card B: "J. Smith", `john@gmail.com`
*   Card C: "Johnny S.", Phone `555-1234` (and `555-1234` belongs to J. Smith too)

### Step 1: Matching
We look for shared information.
*   Card A and Card B share the email `john@gmail.com`. They are stapled together.

### Step 2: Chaining (Label Propagation)
*   Card B also has phone `555-1234`.
*   Card C has phone `555-1234`.
*   So B is linked to C.
*   Result: **A is linked to B, and B is linked to C**. Therefore, A, B, and C are all the same person.

### Step 3: Golden Record
We create a new, clean card for the group.
*   Name: "John Smith" (Best name)
*   Email: "john@gmail.com"
*   Phone: "555-1234"

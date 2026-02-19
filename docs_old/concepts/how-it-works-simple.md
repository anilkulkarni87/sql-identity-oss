# How It Works (Simple Version)

Imagine you have a giant box of business cards on the floor. This is your data.
Some cards are for "John Smith", some for "J. Smith", and some for "Johnny S.".
Your goal is to staple all the cards that belong to the same person together.

This tool does exactly that, in 3 simple steps.

---

## Step 1: Connecting the Dots
First, the tool looks at every card and finds things that are unique, like an **Email Address** or a **Phone Number**.

*   Card A says: `john@gmail.com`
*   Card B says: `john@gmail.com`

**Aha!** Since they have the same email, they must belong to the same person. The tool draws a line connecting Card A and Card B.

## Step 2: Friends of Friends (The "Rounds")
This is the magic part. The tool doesn't just see who is holding hands. It plays a game like "Telephone" or "Pass the Note".

**Round 1:**
*   **John** knows **Jane** (they share an Email).
*   **Jane** knows **Bob** (they share a Phone).

**Round 2:**
*   The tool tells **John**: "Hey, did you know Jane connects to Bob?"
*   Now **John** is connected to **Bob** too!

The tool keeps playing these "Rounds" (we call them **Iterations**) until everyone in the whole group knows everyone else.
Usually, it takes about 5-10 rounds to find every single connection in a huge web of people.

## Step 3: Giving Everyone a Name Tag
Once the tool finds a whole group of cards connected together, it gives them a single **Group ID**.

*   Card A -> Group #123
*   Card B -> Group #123
*   Card C -> Group #123

Now, whenever you ask "Who is Group #123?", the tool shows you a clean profile with the best name ("John Smith") and the best email ("john@gmail.com") from all the cards combined.

---

## Why is this hard?
Sometimes, things get messy.
*   **The Shared Phone**: What if 50 people share the same office phone number? You don't want to staple 50 strangers together!
    *   *Solution*: We tell the tool to ignore "Office Numbers" or "Generic Emails" (like `info@company.com`).
*   **The Typo**: What if one card says `jon@gmail.com` and another says `john@gmail.com`?
    *   *Solution*: We can tell the tool to fix common mistakes before it starts connecting dots.

## Summary
1.  **Find Matches**: Look for shared emails or phones.
2.  **Connect the Dots**: Link friends of friends.
3.  **Group Them**: Give the whole bunch a single ID.

import bz2
import time
from typing import cast

from openai import OpenAI

AI_PROMP_MESSAGE = """
#General context:
The general objective is to rename a statistical variable for DataCommons and categorize it.
There will be an explanation on how to achieve this objective.
Some of the input is compressed with BZ2. For those, please decompress it and interpret as stated.
Throughout this prompt, we will reference variables. The syntax is: <variable>.

The input description is expressed here, but the actual values are stated in the end of this prompt
For the output, I need you to be quiet. Return ONLY the produced from the steps. Do not explain nothing, neither show your process. All the words in the output should be in english

#Input description:
<old_name>: Old variable name to be renamed.
<code>: Code associated to the variable. Use to get more info.
<description>: BZ2 compressed text with the variable description. Use to get more info.
<theme>: Variable theme.
<name_history>: BZ2 compressed list of updated names already computed
<category_history>: BZ2 compressed list of categories already computed

#Task:
I need you to extract some information from the input, and use it to give me an specific output.
This output is very important so you must ensure that all formating is done exactly as I described.

I will state a set of rules that you need to follow to retrieve the information I want.

For the rules below, those marked with "!" are VERY IMPORTANT and not following it will cause
a catastrophe. You have been warned.

##Rules:
- RULE 1: The statistical variable needs to be categorized.
          The category follows an hierarchical structure
          For example, one variable can be categorized like this: "Production/Livestock/Birds/Slaughter"
          Each "node" in the string helps to define the groups the statistical variable is inserted
          And the groups ranges from the more general to the more specific from left to right

          Read all the input as context and read the <category_history> to see the existing defined groups
          Try to reuse one existing group for the variable, but if not possible, create a new one.

          For the wording/terminology, try to use <category_history> and <name_history> names if any to your help

          Save the category hierarchy string in *english* into your memory as the variable <category>
          IMPORTANT: All the nodes need to be in english
          IMPORTANT: The <theme>, translated to english, is required to be in the root of the hierarchy string. In othe words, the first group.
          IMPORTANT: keep the wording consistent. if there is a direct synonym of a group in the category you picket up
                     in either the <name_history> or <category_history>, use the synonym.
          IMPORTANT: the consistency requirement also applies to the translated theme

          Example 1:
            <theme> = "Produçao"
            <category_history> = ["Production/Livestock/Birds/Slaughter"]

- RULE 2: The <old_name> may contain adjectives (english or portuguese) which indicates that the stats var is deprecated.
          They may be expressed in any casing.
          IF AND ONLY IF such adjective is present
              THEN create a variable <is_deprecated> = True
              AND a variable <deprecation_text> containing the adjective
          ELSE
              <is_deprecated> = False
              <deprecation_text> = ''

          Example 1:
              <old_name> = "Abate - vitelos - qde. - INATIVA"
              <is_deprecated> = True
              <deprecation_text> = 'INATIVA'

          Example 2:
              <old_name> = "Abate - vitelos - qde. - Discontinued"
              <is_deprecated> = True
              <deprecation_text> = 'Discontinued'

          Example 3:
              <old_name> = "Abate - vitelos - qde."
              <is_deprecated> = False
              <deprecation_text> = ''

- !RULE 3: Using all the input as context, provide a better name to the variable
           The words of the name should be separated with spaces, and should be in capitalized case
           Do not include ANY deprecation text in the new name.
           Specially, <deprecation_text>, if not empty string, should NOT be in the new name.
           Save it in *english* into your memory as the variable <new_name>
           IMPORTANT: the value of the variable <new_name> should be a name in english
           IMPORTANT: keep the wording consistent. if there is a direct synonym of the name you picket up
                      in either the <name_history> or <category_history>, use the synonym.

           Example:
              <old_name> = "Abate - vacas - qtd"
              *... more context omitted ...*

              <new_name> = "Quantity of Cow Slaughtered"

           Example 2:
              <old_name> = "Abate - vacas - peso"
              *... more context omitted ...*

              <new_name> = "Weight of Slaugtered Cow Carcasses"

- !RULE 5: Inspect the names inside the variable <name_history>
           Check if we can update <new_name> to be equal to one of those
           They need to have the EXACT same meaning, describing the exact same
              statistical variable
           If <previous_names> has a matching name, then update <new_name>, else keep it as it was
           IMPORTANT: <new_name> should be a name in english. Translate if needed

#Output:
Please return the string representations of <final_name> and <category> sepparated by the char "|", without spaces between
Translate it all to english
Something like this: str(<new_name>)|str(<category>)|str(<is_deprecated>)
Obs.: "str()" represents you should evaluate the variable to an string. do not write the str() func to the outpuy

DO NOT RETURN NOTHING MORE. JUST THIS STRING ABOVE
ALSO MAKE SURE ALL COMPONENTS OF THE STRING ARE IN ENGLISH. IF NOT, TRANSLATE IT

Input Values:
<old_name> = {0}
<code> = {1}
<description> = {2}
<theme> = {3}
<name_history> = {4}
<category_history> = {5}
"""


class GptInterface:
    client: OpenAI
    name_history: list[str]
    category_history: list[str]

    def __init__(self):
        self.client = OpenAI(
            api_key="sk-proj-Aylcx6tamb20CwOPEfIkT3BlbkFJYIM8HaaZgEjrW0KsRyf2"
        )
        self.name_history = []
        self.category_history = []

    def prompt(self, name: str, code: str, description: str, theme: str):
        answer: str | None = None
        new_name: str | None = None
        group: str | None = None
        while True:
            try:
                answer = self.__prompt(name, code, description, theme)
                print(f"{answer=}")
                assert isinstance(answer, str)

                new_name, group, is_deprecated = answer.split("|")
                print(f"old_name={name} {new_name=} {group=} {is_deprecated=}")

                self.name_history.append(new_name)
                self.category_history.append(group)

                print("\n\n")
                break
            except Exception as e:
                print(e)
                time.sleep(3)

        if is_deprecated == "True":
            new_name = f"[deprecated] {new_name}"

        return new_name, group

    def __prompt(self, name: str, code: str, description: str, theme: str):
        completion = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            temperature=0.0001,
            seed=4,
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert on DataCommons, and a extremely capable data scientist.",
                },
                {
                    "role": "user",
                    "content": AI_PROMP_MESSAGE.format(
                        name,
                        code,
                        self.compress(description),
                        theme,
                        self.compress(self.name_history),
                        self.compress(self.category_history),
                    ),
                },
            ],
        )

        return cast(str, completion.choices[0].message.content)

    @staticmethod
    def compress(text: list[str] | str) -> str:
        if isinstance(text, list):
            text = "\n".join(text)

        return bz2.compress(text.encode("utf-8")).hex()

    @staticmethod
    def decompress(compressed_text: str) -> list[str] | str:
        decompressed = bz2.decompress(bytes.fromhex(compressed_text)).decode("utf-8")
        if "\n" in decompressed:
            return decompressed.split("\n")

        return decompressed

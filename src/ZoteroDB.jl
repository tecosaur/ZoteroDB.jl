"""
    ZoteroDB

A library for directly interfacing with Zotero SQLite databases.

This module provides a simple interface for accessing Zotero libraries, collections,
items, and attachments. It also provides utility functions for extracting information
from items, such as DOIs and URLs.

# Types

- `Library`: A Zotero library (personal or group) within a Zotero SQLite database.
- `Collection`: A collection within a Zotero library.
- `Item`: An item within a Zotero library.
- `Attachment`: An attachment within a Zotero library.
- `Note`: A note within a Zotero library.
- `Annotation`: A highlight or other annotation made on an attachment.
- `Creator`: An author, editor, or other contributor to an item.
- `Identifier{kind}`: A unique identifier for an entity of type `kind` (`Collection`/`Item`/`Attachment`).

# Functions

Only the `Library`, `Collection`, `Item`, and `Attachment` types are exported.
Everything else is public but called qualified, as `ZoteroDB.items(lib)`, or
brought in by name with `using ZoteroDB: items, collections`.

- `libraries`: Find the personal and group libraries of a database.
- `collections`: Find all collections in a library, or those holding an item.
- `items`: Find all items in a library or collection.
- `children`: Find the notes, attachments, and annotations beneath an item.
- `notes`: Find all notes for an item or in a library.
- `annotations`: Find all annotations for an item or attachment, or in a library.
- `creators`: Find the authors, editors, and other contributors to an item.
- `citationkey`: Find the citation key of an item (if recorded).
- `parent`: Find the item (or attachment) that an entry sits beneath.
- `attachments`: Find all attachments for an item or collection.
- `tags`: Find all tags for an item or in a library.
- `iteminfo`: Find all item information for an item or in a library.
- `url`: Find the URL of an item (if recorded).
- `doi`, `arxiv`, `pmid`, `pmcid`: Find an identifier of an item (if recorded).
- `isbns`, `issns`: Find all the ISBNs or ISSNs of an item.

Identifiers are the types of AcademicIdentifiers.jl, parsed from the item's
dedicated field or else from a `Name: value` line of its "extra" field.

# Example usage

```julia-repl
julia> lib = Library()
Library("/home/nostradamus/Zotero/zotero.sqlite", "My Library")

julia> ZoteroDB.collections(lib)
4-element Vector{Collection}:
 Collection(#1, Everything, children = [Psychic powers#2, Time travel#3, Julia#4])
 Collection(#2, Psychic powers, parent = Everything#1)
 Collection(#3, Time travel, parent = Everything#1)
 Collection(#4, Julia, parent = Everything#1)

julia> ZoteroDB.items(Collection(lib, 2))
3-element Vector{Item}:
 Item(#22, journalArticle, A history of psychic powers)
 Item(#23, soothsaying, The future of psychic powers)
 Item(#24, book, The psychic powers, here and now)

julia> ZoteroDB.attachments(Item(lib, 22))
2-element Vector{Attachment}:
 Attachment(#25, text/html, /home/nostradamus/Zotero/storage/P4CXZUN5/future-psych.html)
 Attachment(#26, application/pdf, /home/nostradamus/Zotero/storage/NHPR2XBJ/future-psych.pdf)
```
"""
module ZoteroDB

using SQLite, DBInterface, Tables, Dates
using StyledStrings
using AcademicIdentifiers: ArXiv, DOI, ISBN, ISSN, PMCID, PMID

export Library, Collection, Item, Attachment

public Note, Annotation, Creator,
    libraries, collections, items, children, attachments, notes, annotations,
    creators, tags, iteminfo, citationkey,
    url, doi, arxiv, pmid, pmcid, isbns, issns

# Types

"""
    Library([dbfile::AbstractString]) -> Library

The personal library ("My Library") of the Zotero SQLite database `dbfile`,
which defaults to `~/Zotero/zotero.sqlite`. Group libraries are found with
[`libraries`](@ref).

The database is only ever read. Throws `AbsentZoteroDBFile` when `dbfile` does not exist.

A library can be opened while Zotero is running, but a query that overlaps
with Zotero writing to the database may fail or see a partial change: run it again.

## Fields

```julia
path::String
db::SQLite.DB
id::Int
name::String
```
"""
struct Library
    path::String
    db::SQLite.DB
    id::Int
    name::String
end

struct Identifier{kind}
    val::Int
end

"""
    Collection([lib::Library], id) -> Collection

A `Collection` within a certain library `lib`, identified by `id` (either an
`Identifier{Collection}` or `Int`).

## Fields

```julia
library::Library
id::Identifier{Collection}
name::String
key::String
parent::Union{Collection, Nothing}
children::Vector{Collection}
```
"""
struct Collection
    library::Library
    id::Identifier{Collection}
    name::String
    key::String
    parent::Union{Collection, Nothing}
    children::Vector{Collection}
end

"""
    Item([lib::Library], id) -> Item

An `Item` within a certain library `lib`, identified by `id`: an
`Identifier{Item}`, an `Int`, or the item's Zotero key as a string.

Any item can be looked up this way, including the notes, attachments,
annotations, and trashed items that `items` leaves out.

## Fields

```julia
library::Library
id::Identifier{Item}
type::Symbol
title::Union{String, Nothing}
added::DateTime
modified::DateTime
key::String
version::Int
synced::Bool
```
"""
struct Item
    library::Library
    id::Identifier{Item}
    type::Symbol
    title::Union{String, Nothing}
    added::DateTime
    modified::DateTime
    key::String
    version::Int
    synced::Bool
end

"""
    Attachment([lib::Library], id) -> Attachment

An `Attachment` within a certain library `lib`, identified by `id`: an
`Identifier{Attachment}`, an `Int`, or the attachment's Zotero key as a string.

Standalone attachments have no `parent`, and `file` is only known for
attachments kept in Zotero's own storage.

## Fields

```julia
library::Library
parent::Union{Identifier{Item}, Nothing}
id::Identifier{Attachment}
linkmode::Int
mime::Union{MIME, Nothing}
file::Union{String, Nothing}
syncState::Int
mtime::Union{Int, Nothing}
hash::Union{String, Nothing}
```
"""
struct Attachment
    library::Library
    parent::Union{Identifier{Item}, Nothing}
    id::Identifier{Attachment}
    linkmode::Int
    mime::Union{MIME, Nothing}
    file::Union{String, Nothing}
    syncState::Int
    mtime::Union{Int, Nothing}
    hash::Union{String, Nothing}
end

"""
    Note

A note within a certain library, either standalone or beneath a `parent` item.

## Fields

```julia
library::Library
parent::Union{Identifier{Item}, Nothing}
id::Identifier{Note}
title::String
html::String
```
"""
struct Note
    library::Library
    parent::Union{Identifier{Item}, Nothing}
    id::Identifier{Note}
    title::String
    html::String
end

"""
    Annotation

An annotation made on the attachment `parent`, of `kind` `:highlight`,
`:underline`, `:note`, `:text`, `:image`, or `:ink`.

The `text` is the passage marked, and the `comment` what was written about it.

## Fields

```julia
library::Library
parent::Identifier{Attachment}
id::Identifier{Annotation}
kind::Symbol
text::Union{String, Nothing}
comment::Union{String, Nothing}
color::Union{String, Nothing}
page::Union{String, Nothing}
```
"""
struct Annotation
    library::Library
    parent::Identifier{Attachment}
    id::Identifier{Annotation}
    kind::Symbol
    text::Union{String, Nothing}
    comment::Union{String, Nothing}
    color::Union{String, Nothing}
    page::Union{String, Nothing}
end

"""
    Creator

A contributor to an item, in the `role` Zotero records (`:author`, `:editor`, …).

A name Zotero holds as a single field, such as an institution's, is the
`family` name with an empty `given` name.

## Fields

```julia
role::Symbol
given::String
family::String
```
"""
struct Creator
    role::Symbol
    given::String
    family::String
end

struct EntryNotFound{K} <: Exception
    needle::Union{Identifier{K}, String}
    library::Library
end

struct UnrootedCollections <: Exception
    library::Library
    ids::Vector{Int}
end

struct AbsentZoteroDBFile <: Exception
    path::String
end

# Base methods

Base.Int(id::Identifier) = id.val
Base.convert(::Type{Int}, id::Identifier) = id.val
Base.convert(::Type{Identifier{K}}, val::Int) where K = Identifier{K}(val)

function Base.iterate(col::Collection, (i, allitems) = (1, items(col)))
    if i > length(allitems)
        nothing
    else
        allitems[i], (i + 1, allitems)
    end
end

Base.eltype(::Type{Collection}) = Item

# Collections compare by identity, as two fetches of one collection hold distinct `children` vectors.
Base.:(==)(a::Collection, b::Collection) = a.id == b.id && a.library === b.library
Base.hash(col::Collection, h::UInt) = hash(col.id, hash(col.library, h))
Base.IteratorSize(::Type{Collection}) = Base.SizeUnknown()

"""
    parent(entry::Union{Item, Attachment, Note}) -> Union{Item, Nothing}
    parent(annotation::Annotation) -> Attachment

Return what `entry` sits beneath, the inverse of [`children`](@ref).
Regular items and standalone notes and attachments have no parent.
"""
function Base.parent(item::Item)
    query = DBInterface.execute(item.library.db, """
        SELECT parentItemID FROM (
            SELECT parentItemID FROM itemAttachments WHERE itemID = ?1 UNION ALL
            SELECT parentItemID FROM itemNotes WHERE itemID = ?1 UNION ALL
            SELECT parentItemID FROM itemAnnotations WHERE itemID = ?1)
        WHERE parentItemID IS NOT NULL LIMIT 1""", [item.id.val])
    parents = Int[row.parentItemID for row in query]
    if !isempty(parents) Item(item.library, first(parents)) end
end

Base.parent(entry::Union{Attachment, Note}) =
    if !isnothing(entry.parent) Item(entry.library, entry.parent) end

Base.parent(annotation::Annotation) = Attachment(annotation.library, annotation.parent)

# Base methods: custom type display

Base.show(io::IO, id::Identifier{K}) where K =
    print(io, styled"$(nameof(K)){shadow:{bold:#}$(id.val)}")

function Base.show(io::IO, lib::Library)
    print(io, "Library(")
    show(io, lib.path)
    print(io, ", ")
    show(io, lib.name)
    print(io, ')')
end

function Base.show(io::IO, col::Collection)
    print(io, styled"Collection({shadow:{bold:#}$(col.id.val)}, {emphasis:$(col.name)}")
    if !isnothing(col.parent)
        print(io, styled", {light:parent} = {bright_blue:$(col.parent.name)}{shadow:{bold:#}$(col.parent.id.val)}")
    end
    if !isempty(col.children)
        print(io, styled", {light:children} = [",
              join(map(c -> styled"{bright_magenta:$(c.name)}{shadow:{bold:#}$(Int(c.id))}", col.children), ", "),
              ']')
    end
    print(io, ')')
end

function Base.show(io::IO, item::Item)
    print(io, styled"Item({shadow:{bold:#}$(item.id.val)}, {bright_magenta:$(item.type)}")
    isnothing(item.title) || print(io, styled", {bright_green:$(item.title)}")
    print(io, ')')
end

function Base.show(io::IO, att::Attachment)
    print(io, styled"Attachment({shadow:{bold:#}$(att.id.val)}, {bright_yellow:$(something(att.mime, '?'))}")
    if isnothing(att.file)
        print(io, ", -")
    elseif isfile(att.file)
        print(io, styled", {underline,link=$(\"file://\" * gethostname() * att.file):$(att.file)}")
    else
        print(io, styled", {bright_red:$(att.file)}")
    end
    print(io, ')')
end

Base.show(io::IO, note::Note) =
    print(io, styled"Note({shadow:{bold:#}$(note.id.val)}, {bright_green:$(note.title)})")

function Base.show(io::IO, ann::Annotation)
    print(io, styled"Annotation({shadow:{bold:#}$(ann.id.val)}, {bright_magenta:$(ann.kind)}")
    isnothing(ann.page) || print(io, styled", {light:p. $(ann.page)}")
    isnothing(ann.text) || print(io, styled", {italic:$(repr(ann.text))}")
    isnothing(ann.comment) || print(io, styled", {bright_green:$(ann.comment)}")
    print(io, ')')
end

function Base.showerror(io::IO, (; needle, library)::EntryNotFound{K}) where K
    print(io, "EntryNotFound: no ", nameof(K))
    if needle isa String
        print(io, if K === Collection " named " else " with key " end, repr(needle))
    else
        print(io, " with ID #", needle.val)
    end
    print(io, " in ", library)
end

function Base.showerror(io::IO, (; library, ids)::UnrootedCollections)
    print(io, "UnrootedCollections: the collections ", join(map(id -> "#$id", ids), ", "),
          " in ", library,
          " descend from no top-level collection (a parent is absent or cyclic), so the database is likely corrupt")
end

function Base.showerror(io::IO, (; path)::AbsentZoteroDBFile)
    print(io, "AbsentZoteroDBFile: no Zotero database at ", path,
          "; give the location of zotero.sqlite with `Library(path)`")
end

# Simple utility methods

Identifier(item::Item) = item.id
Identifier(col::Collection) = col.id
Identifier(att::Attachment) = att.id

const Z_DATE_FORMAT = dateformat"Y-m-d H:M:S"

const DEFAULT_LIBRARY = Ref{Library}()

function default_library()
    if !isassigned(DEFAULT_LIBRARY)
        DEFAULT_LIBRARY[] = Library()
    end
    DEFAULT_LIBRARY[]
end

# API

"""
    libraries([dbfile::AbstractString]) -> Vector{Library}

Return the libraries of the Zotero SQLite database `dbfile`: the personal
library first, then each group library by name.

See [`Library`](@ref) for the default `dbfile` and how the database is opened.
"""
function libraries(dbfile::AbstractString = joinpath(homedir(), "Zotero", "zotero.sqlite"))
    path = abspath(dbfile)
    isfile(path) || throw(AbsentZoteroDBFile(path))
    # Zotero holds an exclusive lock while running, which only an immutable open ignores.
    db = SQLite.DB(string("file:", replace(path, '%' => "%25", '?' => "%3f", '#' => "%23"), "?immutable=1"))
    query = DBInterface.execute(db, """
        SELECT libraryID, coalesce(name, 'My Library') AS name
        FROM libraries LEFT JOIN groups USING (libraryID)
        WHERE type IN ('user', 'group') ORDER BY type DESC, name""")
    Library[Library(path, db, row.libraryID, row.name) for row in query]
end

Library(dbfile::AbstractString) = first(libraries(dbfile))
Library() = first(libraries())

"""
    collections(lib::Library = default) -> Vector{Collection}
    collections(item::Item)

Return a vector of all collections in the library `lib`, or of those that hold `item`.
"""
function collections(lib::Library)
    rows = collect(Tables.namedtupleiterator(DBInterface.execute(lib.db, "SELECT * from collections WHERE libraryID = ?", [lib.id])))
    subrows = Dict{Union{Int, Missing}, Vector{Int}}()
    for (i, row) in pairs(rows)
        push!(get!(Vector{Int}, subrows, row.parentCollectionID), i)
    end
    found = Collection[]
    function descend!(parent, parentid)
        for row in view(rows, get(subrows, parentid, Int[]))
            col = Collection(lib, Identifier{Collection}(row.collectionID), row.collectionName, row.key, parent, Collection[])
            isnothing(parent) || push!(parent.children, col)
            push!(found, col)
            descend!(col, row.collectionID)
        end
    end
    descend!(nothing, missing)
    length(found) == length(rows) ||
        throw(UnrootedCollections(lib, setdiff(map(r -> r.collectionID, rows), map(c -> c.id.val, found))))
    sort!(found, by = c -> c.id.val)
end

collections() = collections(default_library())

function collections(item::Item)
    query = DBInterface.execute(item.library.db, "SELECT collectionID FROM collectionItems WHERE itemID = ?", [item.id.val])
    holders = Set{Int}(row.collectionID for row in query)
    filter(col -> col.id.val in holders, collections(item.library))
end

function Collection(lib::Library, id::Identifier{Collection})
    # We'll get all collections to ensure the parent field is resolved correctly
    allcollections = collections(lib)
    idx = findfirst(c -> c.id == id, allcollections)
    if isnothing(idx)
        throw(EntryNotFound(id, lib))
    else
        allcollections[idx]
    end
end

Collection(lib::Library, id::Int) = Collection(lib, Identifier{Collection}(id))

function Collection(lib::Library, name::AbstractString)
    allcollections = collections(lib)
    idx = findfirst(c -> c.name == name, allcollections)
    if isnothing(idx)
        throw(EntryNotFound{Collection}(String(name), lib))
    else
        allcollections[idx]
    end
end

Collection(id::Union{Identifier{Collection}, Int, <:AbstractString}) = Collection(default_library(), id)

"""
    items(lib::Library = default; [tag]) -> Vector{Item}
    items([lib::Library = default], collection::Identifier{Collection})
    items(collection::Collection)

Return all `Item`s in either a given `library` or `collection`, optionally
only those of a `library` that carry a certain `tag`.

Only regular items are returned: notes, attachments, and annotations are found
through `children`, and items in the trash are left out.
"""
function items end

const ITEM_QUERY = """
    SELECT items.*, typeName, coalesce(
        (SELECT value FROM itemData JOIN itemDataValues USING (valueID) JOIN fields USING (fieldID)
         WHERE itemData.itemID = items.itemID AND fieldName = 'title'),
        (SELECT title FROM itemNotes WHERE itemNotes.itemID = items.itemID)) AS title
    FROM items JOIN itemTypes USING (itemTypeID)"""

const UNTRASHED = "itemID NOT IN (SELECT itemID FROM deletedItems)"
const REGULAR_ITEM = "typeName NOT IN ('attachment', 'note', 'annotation') AND $UNTRASHED"

function items(lib::Library = default_library(); tag::Union{AbstractString, Nothing} = nothing)
    isnothing(tag) && return _items(lib, REGULAR_ITEM)
    _items(lib, "$REGULAR_ITEM AND itemID IN (SELECT itemID FROM itemTags JOIN tags USING (tagID) WHERE name = ?)", tag)
end

items(lib::Library, col::Identifier{Collection}) =
    _items(lib, "$REGULAR_ITEM AND itemID IN (SELECT itemID FROM collectionItems WHERE collectionID = ?)", col.val)

items(col::Collection) = items(col.library, col.id)

items(col::Identifier{Collection}) = items(default_library(), col)

"""
    children(item::Item) -> Vector{Item}

Return the items beneath `item`: the notes and attachments of a regular item,
or the annotations of an attachment. Those in the trash are left out.
"""
children(item::Item) =
    _items(item.library, """$UNTRASHED AND itemID IN (
        SELECT itemID FROM itemAttachments WHERE parentItemID = ?1 UNION
        SELECT itemID FROM itemNotes WHERE parentItemID = ?1 UNION
        SELECT itemID FROM itemAnnotations WHERE parentItemID = ?1)""", item.id.val)

function Item(lib::Library, id::Identifier{Item})
    found = _items(lib, "itemID = ?", id.val)
    isempty(found) && throw(EntryNotFound(id, lib))
    first(found)
end

function Item(lib::Library, key::AbstractString)
    found = _items(lib, "items.key = ?", key)
    isempty(found) && throw(EntryNotFound{Item}(String(key), lib))
    first(found)
end

Item(lib::Library, id::Int) = Item(lib, Identifier{Item}(id))
Item(id::Union{Identifier{Item}, Int, AbstractString}) = Item(default_library(), id)

function _items(lib::Library, predicate::String, params...)
    query = DBInterface.execute(lib.db, "$ITEM_QUERY WHERE ($predicate) AND items.libraryID = ? ORDER BY itemID", (params..., lib.id))
    Item[Item(lib, row.itemID, Symbol(row.typeName), coalesce(row.title, nothing),
              parse(DateTime, row.dateAdded, Z_DATE_FORMAT),
              parse(DateTime, row.dateModified, Z_DATE_FORMAT),
              row.key, row.version, Bool(row.synced))
         for row in Tables.namedtupleiterator(query)]
end

"""
    creators(item::Item) -> Vector{Creator}

Return the contributors to `item`, in the order Zotero lists them.
"""
function creators(item::Item)
    query = DBInterface.execute(item.library.db, """
        SELECT creatorType, firstName, lastName FROM itemCreators
        JOIN creators USING (creatorID) JOIN creatorTypes USING (creatorTypeID)
        WHERE itemID = ? ORDER BY orderIndex""", [item.id.val])
    Creator[Creator(Symbol(row.creatorType), coalesce(row.firstName, ""), coalesce(row.lastName, ""))
            for row in Tables.namedtupleiterator(query)]
end

const ATTACHMENT_QUERY = "SELECT items.key, itemAttachments.* FROM itemAttachments JOIN items USING (itemID)"

function Attachment(lib::Library, att::Identifier{Attachment})
    found = _attachments(lib, "itemID = ?", att.val)
    isempty(found) && throw(EntryNotFound(att, lib))
    first(found)
end

function Attachment(lib::Library, key::AbstractString)
    found = _attachments(lib, "items.key = ?", key)
    isempty(found) && throw(EntryNotFound{Attachment}(String(key), lib))
    first(found)
end

Attachment(lib::Library, id::Int) = Attachment(lib, Identifier{Attachment}(id))

Attachment(att::Union{Identifier{Attachment}, Int, AbstractString}) = Attachment(default_library(), att)

"""
    attachments([lib::Library], item/collection) -> Vector{Attachment}
    attachments(lib::Library = default)

Return all attachments for an `item` or `collection` (given as an
`Item`/`Collection` or `Identifier{Item}`/`Identifier{Collection}`) in a given
Library `lib`. Attachments in the trash are left out.
"""
function attachments end

attachments(item::Item) = _attachments(item.library, "$UNTRASHED AND parentItemID = ?", item.id.val)

attachments(lib::Library, item::Identifier{Item}) = attachments(Item(lib, item))
attachments(item::Identifier{Item}) = attachments(default_library(), item)

# A standalone attachment is itself the collection's item, not the child of one.
attachments(lib::Library, collection::Identifier{Collection}) =
    _attachments(lib, "$UNTRASHED AND EXISTS (SELECT 1 FROM collectionItems WHERE collectionID = ? AND collectionItems.itemID IN (itemAttachments.itemID, parentItemID))", collection.val)
attachments(collection::Identifier{Collection}) = attachments(default_library(), collection)
attachments(col::Collection) = attachments(col.library, col.id)

attachments(lib::Library = default_library()) = _attachments(lib, UNTRASHED)

function _attachments(lib::Library, predicate::String, params...)
    query = DBInterface.execute(lib.db, "$ATTACHMENT_QUERY WHERE ($predicate) AND items.libraryID = ? ORDER BY itemAttachments.itemID", (params..., lib.id))
    map(Tables.namedtupleiterator(query)) do row
        file = if !ismissing(row.path) && startswith(row.path, "storage:")
            joinpath(dirname(lib.path), "storage", row.key, chopprefix(row.path, "storage:"))
        end
        Attachment(lib, if !ismissing(row.parentItemID) Identifier{Item}(row.parentItemID) end,
                   Identifier{Attachment}(row.itemID), row.linkMode,
                   if !ismissing(row.contentType) MIME(row.contentType) end,
                   file, row.syncState,
                   coalesce(row.storageModTime, nothing), coalesce(row.storageHash, nothing))
    end
end

"""
    notes(item::Item) -> Vector{Note}
    notes(lib::Library = default)

Return the notes beneath `item`, or every note in the library `lib`
(standalone ones included). Notes in the trash are left out.
"""
function notes end

notes(item::Item) = _notes(item.library, "parentItemID = ?", item.id.val)
notes(lib::Library = default_library()) = _notes(lib, "TRUE")

function _notes(lib::Library, predicate::String, params...)
    # Attachments also have `itemNotes` rows, which are not notes in their own right.
    query = DBInterface.execute(lib.db, """
        SELECT itemNotes.* FROM itemNotes JOIN items USING (itemID) JOIN itemTypes USING (itemTypeID)
        WHERE typeName = 'note' AND $UNTRASHED AND ($predicate) AND libraryID = ?
        ORDER BY itemID""", (params..., lib.id))
    Note[Note(lib, if !ismissing(row.parentItemID) Identifier{Item}(row.parentItemID) end,
              Identifier{Note}(row.itemID), coalesce(row.title, ""), coalesce(row.note, ""))
         for row in Tables.namedtupleiterator(query)]
end

"""
    annotations(attachment::Attachment) -> Vector{Annotation}
    annotations(item::Item)
    annotations(lib::Library = default)

Return the annotations made on an `attachment`, on any attachment of an `item`,
or anywhere in the library `lib`, in reading order within each attachment.
Annotations in the trash are left out.
"""
function annotations end

annotations(att::Attachment) = _annotations(att.library, "parentItemID = ?", att.id.val)
annotations(item::Item) =
    _annotations(item.library, "?1 IN (parentItemID, (SELECT parentItemID FROM itemAttachments WHERE itemID = itemAnnotations.parentItemID))", item.id.val)
annotations(lib::Library = default_library()) = _annotations(lib, "TRUE")

# Indexed by Zotero's annotation type number.
const ANNOTATION_KINDS = (:highlight, :note, :image, :ink, :underline, :text)

function _annotations(lib::Library, predicate::String, params...)
    query = DBInterface.execute(lib.db, """
        SELECT itemAnnotations.* FROM itemAnnotations JOIN items USING (itemID)
        WHERE $UNTRASHED AND ($predicate) AND libraryID = ?
        ORDER BY parentItemID, sortIndex""", (params..., lib.id))
    Annotation[Annotation(lib, Identifier{Attachment}(row.parentItemID), Identifier{Annotation}(row.itemID),
                          get(ANNOTATION_KINDS, row.type, :unknown),
                          coalesce(row.text, nothing), coalesce(row.comment, nothing),
                          coalesce(row.color, nothing), coalesce(row.pageLabel, nothing))
               for row in Tables.namedtupleiterator(query)]
end

"""
    tags([library::Library], item::Union{Identifier{Item}, Int}) -> Vector{String}
    tags(item::Item)

Return all tags for an `item` in a given `library`.
"""
function tags end

"""
    tags(library::Library = default) -> Dict{Int, String}

Return all tags used in a given `library`, by tag ID.
"""
function tags(lib::Library = default_library())
    query = DBInterface.execute(lib.db, """
        SELECT DISTINCT tagID, name FROM tags JOIN itemTags USING (tagID) JOIN items USING (itemID)
        WHERE libraryID = ?""", [lib.id])
    Dict{Int, String}(row.tagID => row.name for row in query)
end

function tags(lib::Library, item::Identifier{Item})
    query = DBInterface.execute(lib.db, "SELECT name FROM itemTags JOIN tags USING (tagID) WHERE itemID = ? ORDER BY name", [item.val])
    String[row.name for row in query]
end

tags(lib::Library, item::Int) = tags(lib, Identifier{Item}(item))
tags(item::Item) = tags(item.library, item.id)
tags(item::Union{Identifier{Item}, Int}) = tags(default_library(), item)

"""
    iteminfo([library::Library], item::Identifier{Item}) -> Dict{Symbol, String}
    iteminfo([library::Library], item::Int)
    iteminfo(item::Item)

Return a dictionary of item information for a given `item` in a given `library`.
"""
function iteminfo end

const ITEMDATA_QUERY = "SELECT itemID, fieldName, value FROM itemData JOIN fields USING (fieldID) JOIN itemDataValues USING (valueID)"

"""
    iteminfo(library::Library = default) -> Dict{Identifier{Item}, Dict{Symbol, String}}

    iteminfo([library::Library], collection)

Return a dictionary of item information for all items in a given `library`, or
in a `collection` (a `Collection` or `Identifier{Collection}`) of it.

Items of a `collection` with no recorded information map to an empty
dictionary, while the `library` form omits them.
"""
function iteminfo(lib::Library = default_library())
    allinfo = Dict{Identifier{Item}, Dict{Symbol, String}}()
    for row in DBInterface.execute(lib.db, "$ITEMDATA_QUERY JOIN items USING (itemID) WHERE libraryID = ?", [lib.id])
        get!(Dict{Symbol, String}, allinfo, Identifier{Item}(row.itemID))[Symbol(row.fieldName)] = row.value
    end
    allinfo
end

iteminfo(lib::Library, item::Identifier{Item}) =
    Dict{Symbol, String}(Symbol(row.fieldName) => row.value
                         for row in DBInterface.execute(lib.db, "$ITEMDATA_QUERY WHERE itemID = ?", [item.val]))

iteminfo(item::Item) = iteminfo(item.library, item.id)
iteminfo(lib::Library, item::Int) = iteminfo(lib, Identifier{Item}(item))
iteminfo(item::Union{Identifier{Item}, Int}) = iteminfo(default_library(), item)

function iteminfo(lib::Library, col::Identifier{Collection})
    allinfo = iteminfo(lib)
    Dict(item.id => get(allinfo, item.id, Dict{Symbol, String}()) for item in items(lib, col))
end

iteminfo(col::Collection) = iteminfo(col.library, col.id)

"""
    url(item::Item) -> Union{String, Nothing}

Return the URL of an `item`, if it has one.
"""
url(item::Item) = get(iteminfo(item), :url, nothing)

"""
    citationkey(item::Item) -> Union{String, Nothing}

Return the citation key of an `item`, if it has one, whether held in Zotero's
own field or in a `Citation Key:` line of the extra field (as Better BibTeX pins it).
"""
function citationkey(item::Item)
    info = iteminfo(item)
    @something(get(info, :citationKey, nothing), get(extravalues(info, "Citation Key"), 1, nothing), Some(nothing))
end

"""
    doi(item::Item) -> Union{DOI, Nothing}

Return the DOI of an `item`, if a valid one is recorded.

A DOI that is recorded but malformed is treated as absent; the raw text
remains available through `iteminfo`.
"""
doi(item::Item) = get(identifiers(DOI, iteminfo(item), :DOI), 1, nothing)

"""
    pmid(item::Item) -> Union{PMID, Nothing}

Return the PubMed ID of an `item`, if a valid one is recorded in its extra field.
"""
pmid(item::Item) = get(identifiers(PMID, iteminfo(item), :PMID), 1, nothing)

"""
    pmcid(item::Item) -> Union{PMCID, Nothing}

Return the PubMed Central ID of an `item`, if a valid one is recorded in its extra field.
"""
pmcid(item::Item) = get(identifiers(PMCID, iteminfo(item), :PMCID), 1, nothing)

"""
    arxiv(item::Item) -> Union{ArXiv, Nothing}

Return the arXiv identifier of an `item`, if it has one.

It is taken from an `arXiv:` line of the extra field, the archive ID, or an
arxiv.org URL, in that order.
"""
function arxiv(item::Item)
    info = iteminfo(item)
    @something(get(identifiers(ArXiv, info, :arXiv), 1, nothing),
               tryparse(ArXiv, get(info, :archiveID, "")),
               tryparse(ArXiv, get(info, :url, "")),
               Some(nothing))
end

"""
    isbns(item::Item) -> Vector{ISBN}

Return every valid ISBN of an `item`, in the order recorded.

Zotero keeps the ISBNs of all an item's editions in one field.
"""
isbns(item::Item) = identifiers(ISBN, iteminfo(item), :ISBN)

"""
    issns(item::Item) -> Vector{ISSN}

Return every valid ISSN of an `item`, in the order recorded.

Zotero keeps the print and electronic ISSNs in one field.
"""
issns(item::Item) = identifiers(ISSN, iteminfo(item), :ISSN)

function identifiers(::Type{T}, info::Dict{Symbol, String}, field::Symbol) where T
    text = get(() -> join(extravalues(info, String(field)), ' '), info, field)
    T[id for id in Iterators.map(word -> tryparse(T, rstrip(word, (',', ';'))), eachsplit(text)) if !isnothing(id)]
end

# What an item type has no field for is recorded as a `label: value` line of the extra field.
function extravalues(info::Dict{Symbol, String}, label::String)
    prefix = lowercase("$label:")
    String[strip(SubString(line, ncodeunits(prefix) + 1))
           for line in eachsplit(get(info, :extra, ""), '\n') if startswith(lowercase(line), prefix)]
end

end

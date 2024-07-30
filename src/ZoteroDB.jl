"""
    ZoteroDB

A library for directly interfacing with Zotero SQLite databases.

This module provides a simple interface for accessing Zotero libraries, collections,
items, and attachments. It also provides utility functions for extracting information
from items, such as DOIs and URLs.

# Types

- `Library`: A Zotero library, represented by a connection to a Zotero SQLite database.
- `Collection`: A collection within a Zotero library.
- `Item`: An item within a Zotero library.
- `Attachment`: An attachment within a Zotero library.
- `Identifier{kind}`: A unique identifier for an entity of type `kind` (`Collection`/`Item`/`Attachment`).

# Functions

- `collections`: Find all collections in a library.
- `items`: Find all items in a library or collection.
- `attachments`: Find all attachments for an item or collection.
- `tags`: Find all tags for an item or in a library.
- `iteminfo`: Find all item information for an item or in a library.
- `doi`: Find the DOI of an item (if recorded).
- `url`: Find the URL of an item (if recorded).

# Example usage

```julia-repl
julia> lib = Library()
Library("/home/nostradamus/Zotero/zotero.sqlite")

julia> collections()
3-element Vector{Collection}
 Collection(#1, Everything, cchildren = [Psychic powers#2, Time travel#3, Julia#4])
 Collection(#2, Psychic powers, parent = Everything#1)
 Collection(#3, Time travel, parent = Everything#1)
 Collection(#4, Julia, parent = Everything#1)

juila> items(Collection(lib, 2))
 Item(#22, journalArticle, A history of psychic powers)
 Item(#23, soothsaying, The future of psychic powers)
 Item(#24, book, The psychic powers, here and now)

julia> attachments(Item(lib, 22))
 2-element Vector{Attachment}
 Attachment(#22, application/pdf, /home/nostradamus/Zotero/storage/P4CXZUN5/future-psych.html)
 Attachment(#23, application/pdf, /home/nostradamus/Zotero/storage/NHPR2XBJ/future-psych.pdf)
```
"""
module ZoteroDB

using SQLite, DBInterface, Tables, Dates
using StyledStrings

export Library, Collection, Item, Attachment,
    collections, items, attachments, tags, iteminfo, doi, url

# Types

"""
    Library([dbfile::AbstractString]) -> Library

A Zotero library, represented by a connection to a Zotero SQLite database.

## Fields

```julia
db::SQLite.DB
```
"""
struct Library
    db::SQLite.DB
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

An `Item` within a certain library `lib`, identified by `id` (either an
`Identifier{Item}` or an `Int`).

## Fields

```julia
library::Library
id::Identifier{Item}
type::Symbol
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
    added::DateTime
    modified::DateTime
    key::String
    version::Int
    synced::Bool
end

"""
    Attachment([lib::Library], id) -> Attachment

An `Attachment` within a certain library `lib`, identified by `id` (either an
`Identifier{Attachment}` or an `Int`).

## Fields

```julia
library::Library
parent::Identifier{Item}
id::Identifier{Attachment}
linkmode::Int
mime::MIME
file::Union{String, Nothing}
syncState::Int
mtime::Union{Int, Nothing}
hash::Union{String, Nothing}
```
"""
struct Attachment
    library::Library
    parent::Identifier{Item}
    id::Identifier{Attachment}
    linkmode::Int
    mime::MIME
    file::Union{String, Nothing}
    syncState::Int
    mtime::Union{Int, Nothing}
    hash::Union{String, Nothing}
end

struct EntryNotFound <: Exception
    needle::Identifier
    within::Identifier
end

struct AbsentZoteroDBFile <: Exception end

# Base methods

Base.Int(id::Identifier) = id.val
Base.convert(::Type{Int}, id::Identifier) = id.val
Base.convert(::Type{Identifier{K}}, val::Int) where K = Identifier{K}(val)

function Base.iterate(col::Collection)
    allitems = items(col)
    first(allitems), (2, allitems)
end

function Base.iterate(col::Collection, (i, allitems))
    if i > length(allitems)
        nothing
    else
        allitems[i], (i + 1, allitems)
    end
end

Base.IteratorSize(::Type{Collection}) = Base.SizeUnknown()

# Base methods: custom type display

function Base.show(io::IO, ::MIME"text/plain", id::Identifier{K}) where K
    print(io, "$K{shadow:{bold:#}$(id.val)}")
end

function Base.show(io::IO, ::MIME"text/plain", lib::Library)
    print(io, "Library(")
    show(io, lib.db.file)
    print(io, ')')
end

function Base.show(io::IO, ::MIME"text/plain", col::Collection)
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

function Base.show(io::IO, ::MIME"text/plain", item::Item)
    print(io, styled"Item({shadow:{bold:#}$(item.id.val)}, {bright_magenta:$(item.type)}")
    md = iteminfo(item)
    if haskey(md, :title)
        print(io, styled", {bright_green:$(md[:title])}")
    end
    print(io, ')')
end

function Base.show(io::IO, ::MIME"text/plain", att::Attachment)
    print(io, styled"Attachment({shadow:{bold:#}$(att.id.val)}, {bright_yellow:$(att.mime)}")
    if isnothing(att.file)
        print(io, ", -")
    elseif isfile(att.file)
        print(io, styled", {underline,link=$(\"file://\" * gethostname() * att.file):$(att.file)}")
    else
        print(io, styled", {bright_red:$(att.file)}")
    end
    print(io, ')')
end

function Base.showerror(io::IO, err::EntryNotFound)
    ikind(::Identifier{K}) where K = K
    print(io, "EntryNotFound: could not find an $(ikind(err.needle)) with ID #$(err.needle.val)")
    if err.within isa Identifier{Library}
        print(io, " in the library")
    else
        print(io, " within $(err.within)#$(Int(err.within))")
    end
end

# Simple utility methods

Identifier(item::Item) = item.id
Identifier(col::Collection) = col.id
Identifier(att::Attachment) = att.id

const Z_DATE_FORMAT = dateformat"Y-m-d H:M:S"

const DEFAULT_LIBRARY = Ref{Library}()

function find_zotero_db()
    trypath = joinpath(homedir(), "Zotero", "zotero.sqlite")
    isfile(trypath) || throw(AbsentZoteroDBFile())
    trypath
end

function default_library()
    if !isassigned(DEFAULT_LIBRARY)
        DEFAULT_LIBRARY[] = Library()
    end
    DEFAULT_LIBRARY[]
end

# API

Library(dbfile::AbstractString) = Library(SQLite.DB(dbfile))
Library() = Library(find_zotero_db())

"""
    collections(lib::Library = default) -> Vector{Collection}

Return a vector of all collections in the library `lib`.
"""
function collections(lib::Library)
    rows = DBInterface.execute(lib.db, "SELECT * from collections")
    crows = collect(Tables.namedtupleiterator(rows))
    collections = Dict{Int, Collection}()
    while !isempty(crows)
        # Iterate through `crows` and pop all elements that have no parent
        # (indicated by a `missing` parent), or whose parent is already in
        # `collections`.
        i = firstindex(crows)
        while i <= lastindex(crows)
            if ismissing(crows[i].parentCollectionID) || haskey(collections, crows[i].parentCollectionID)
                row = popat!(crows, i)
                parent = if !ismissing(row.parentCollectionID)
                    collections[row.parentCollectionID]
                end
                collections[row.collectionID] = Collection(lib, Identifier{Collection}(row.collectionID), row.collectionName, row.key, parent, Collection[])
                if !isnothing(parent)
                    push!(parent.children, collections[row.collectionID])
                end
            else
                i += 1
            end
        end
    end
    map(last, sort(collect(pairs(collections)), by=first))
end

collections() = collections(default_library())

function Collection(lib::Library, id::Identifier{Collection})
    # We'll get all collections to ensure the parent field is resolved correctly
    allcollections = collections(lib)
    idx = findfirst(c -> c.id == id, allcollections)
    if isnothing(idx)
        throw(EntryNotFound(id, Identifier{Library}(0)))
    else
        allcollections[idx]
    end
end

Collection(lib::Library, id::Int) = Collection(lib, Identifier{Collection}(id))

function Collection(lib::Library, name::AbstractString)
    allcollections = collections(lib)
    idx = findfirst(c -> c.name == name, allcollections)
    if isnothing(idx)
        throw(EntryNotFound(Collection, Identifier{Library}(0)))
    else
        allcollections[idx]
    end
end

Collection(id::Union{Identifier{Collection}, Int, <:AbstractString}) = Collection(default_library(), id)

const _ITEM_TYPES = Dict{Int, Symbol}()
function _itemtypes(lib::Library)
    if isempty(_ITEM_TYPES)
        rows = DBInterface.execute(lib.db, "SELECT * from itemTypes")
        for row in rows
            _ITEM_TYPES[row.itemTypeID] = Symbol(row.typeName)
        end
    end
    _ITEM_TYPES
end

"""
    items(lib::Library = default) -> Vector{Item}
    items([lib::Library = default], collection::Identifier{Collection})
    items(collection::Collection)

Return all `Item`s in either a given `library` or `collection`.
"""
function items end

function items(lib::Library = default_library())
    rows = DBInterface.execute(lib.db, "SELECT * from items")
    itypes = _itemtypes(lib)
    map(Tables.namedtupleiterator(rows)) do row
        Item(lib, row.itemID, get(itypes, row.itemTypeID, :unknown),
             parse(DateTime, row.dateAdded, Z_DATE_FORMAT),
             parse(DateTime, row.dateModified, Z_DATE_FORMAT),
             row.key, row.version, Bool(row.synced))
    end
end

function items(lib::Library, col::Identifier{Collection})
    rows = DBInterface.execute(lib.db, "SELECT * from collectionItems WHERE collectionID = ?", [col.val])
    citems = map(r -> Identifier{Item}(r.itemID), rows)
    filter(i -> i.id in citems, items(lib))
end

items(col::Collection) = items(col.library, col.id)

items(col::Identifier{Collection}) = items(default_library(), col)

function Item(lib::Library, id::Identifier{Item})
    query = DBInterface.execute(lib.db, "SELECT * from items WHERE itemID = ?", [id.val])
    result = collect(Tables.namedtupleiterator(query))
    if isempty(result)
        throw(EntryNotFound(id, Identifier{Library}(0)))
    else
        row = first(result)
        itypes = _itemtypes(lib)
        Item(lib, id, get(itypes, row.itemTypeID, :unknown),
             parse(DateTime, row.dateAdded, Z_DATE_FORMAT),
             parse(DateTime, row.dateModified, Z_DATE_FORMAT),
             row.key, row.version, Bool(row.synced))
    end
end

Item(lib::Library, id::Int) = Item(lib, Identifier{Item}(id))
Item(id::Union{Identifier{Item}, Int}) = Item(default_library(), id)

function Attachment(lib::Library, att::Identifier{Attachment})
    kquery = DBInterface.execute(lib.db, "SELECT key from items WHERE itemID = ?", [att.val])
    isempty(kquery) && throw(EntryNotFound(att, Identifier{Library}(0)))
    key = first(kquery).key
    attquery = DBInterface.execute(lib.db, "SELECT * from itemAttachments WHERE itemID = ?", [att.val])
    isempty(attquery) && throw(EntryNotFound(att, Identifier{Library}(0)))
    attdata = first(attquery)
    path = if !ismissing(attdata.path) && startswith(attdata.path, "storage:")
        joinpath(dirname(lib.db.file), "storage", key, chopprefix(attdata.path, "storage:"))
    end
    Attachment(lib, Identifier{Item}(attdata.parentItemID), att, attdata.linkMode,
               MIME(attdata.contentType), path, attdata.syncState,
               if !ismissing(attdata.storageModTime) attdata.storageModTime end,
               if !ismissing(attdata.storageHash) attdata.storageHash end)
end

Attachment(lib::Library, id::Int) = Attachment(lib, Identifier{Attachment}(id))

Attachment(att::Union{Identifier{Attachment}, Int}) = Attachment(default_library(), att)

"""
    attachments([lib::Library], item/collection) -> Vector{Attachment}
    attachments(lib::Library = default)

Return all attachments for an `item` or `collection` (given as an
`Item`/`Collection` or `Identifier{Item}`/`Identifier{Collection}`) in a given
Library `lib`.
"""
function attachments end

function attachments(item::Item)
    query = DBInterface.execute(item.library.db, "SELECT itemID from itemAttachments WHERE parentItemID = ?", [Int(item.id)])
    map(r -> Attachment(item.library, Identifier{Attachment}(r.itemID)), Tables.namedtupleiterator(query))
end

attachments(lib::Library, item::Identifier{Item}) = attachments(Item(lib, item))
attachments(item::Identifier{Item}) = attachments(default_library(), item)

attachments(lib::Library, collection::Identifier{Collection}) =
    map(attachments, items(lib, collection)) |> Iterators.flatten |> collect
attachments(collection::Identifier{Collection}) = attachments(default_library(), collection)
attachments(col::Collection) = attachments(col.library, col.id)

attachments(lib::Library = default_library()) =
    map(attachments, items(lib)) |> Iterators.flatten |> collect

"""
    tags([library::Library], item::Union{Identitier{Item}, Int}) -> Dict{Int, String}
    tags(item::Item)

Return all tags for an `item` in a given `library`.
"""
function tags end

"""
    tags(library::Library = default)

Return all tags used in a given `library`.
"""
function tags(lib::Library = default_library())
    query = DBInterface.execute(lib.db, "SELECT * from tags")
    map(Tables.namedtupleiterator(query)) do row
        row.tagID => row.name
    end |> Dict{Int, String}
end

function tags(lib::Library, item::Identifier{Item})
    alltags = tags(lib)
    query = DBInterface.execute(lib.db, "SELECT * from itemTags WHERE itemID = ?", [item.val])
    map(Tables.namedtupleiterator(query)) do row
        alltags[row.tagID]
    end
end

tags(lib::Library, item::Int) = tags(lib, Identifier{Item}(item))
tags(item::Item) = tags(item.library, item.id)
tags(item::Union{Identifier{Item}, Int}) = tags(default_library(), item)

const _FIELDS = Dict{Int, Symbol}()
function _libfields(lib::Library)
    if isempty(_FIELDS)
        query = DBInterface.execute(lib.db, "SELECT * from fields")
        for row in Tables.namedtupleiterator(query)
            _FIELDS[row.fieldID] = Symbol(row.fieldName)
        end
    end
    _FIELDS
end

"""
    iteminfo([library::Library], item::Identifier{Item}) -> Dict{Symbol, String}
    iteminfo([library::Library], item::Int)
    iteminfo(item::Item)

Return a dictionary of item information for a given `item` in a given `library`.
"""
function iteminfo end

"""
    iteminfo(library::Library = default) -> Dict{Identifier{Item}, Dict{Symbol, String}}

Return a dictionary of item information for all items in a given `library`.
"""
function iteminfo(lib::Library = default_library())
    ifields = _libfields(lib)
    values = Dict{Int, String}()
    vquery = DBInterface.execute(lib.db, "SELECT * from itemDataValues")
    for row in Tables.namedtupleiterator(vquery)
        values[row.valueID] = row.value
    end
    iteminfo = Dict{Identifier{Item}, Dict{Symbol, String}}()
    rquery = DBInterface.execute(lib.db, "SELECT * from itemData")
    for row in Tables.namedtupleiterator(rquery)
        itemid = Identifier{Item}(row.itemID)
        if !haskey(iteminfo, row.itemID)
            iteminfo[itemid] = Dict{Symbol, String}()
        end
        iteminfo[itemid][ifields[row.fieldID]] = values[row.valueID]
    end
    iteminfo
end

function iteminfo(lib::Library, item::Identifier{Item})
    query = DBInterface.execute(lib.db, "SELECT * from itemData WHERE itemID = ?", [item.val])
    results = collect(Tables.namedtupleiterator(query))
    ifields = _libfields(lib)
    values = Dict{Int, String}()
    vquery = DBInterface.execute(lib.db, "SELECT * from itemDataValues WHERE valueID in ($(join(fill('?', length(results)), ',')))", map(r -> r.valueID, results))
    for row in Tables.namedtupleiterator(vquery)
        values[row.valueID] = row.value
    end
    map(results) do row
        ifields[row.fieldID] => values[row.valueID]
    end |> Dict{Symbol, String}
end

iteminfo(item::Item) = iteminfo(item.library, item.id)
iteminfo(lib::Library, item::Int) = iteminfo(lib, Identifier{Item}(item))
iteminfo(item::Union{Identifier{Item}, Int}) = iteminfo(default_library(), item)

function iteminfo(lib::Library, col::Identifier{Collection})
    rows = DBInterface.execute(lib.db, "SELECT * from collectionItems WHERE collectionID = ?", [col.value])
    allinfo = iteminfo(lib)
    map(rows) do row
        allinfo[row.itemID]
    end
end

iteminfo(col::Collection) = iteminfo(col.library, col.id)

"""
    doi(item::Item) -> Union{String, Nothing}

Return the DOI of an `item`, if it has one.
"""
doi(item::Item) = get(iteminfo(item), :DOI, nothing)

"""
    url(item::Item) -> Union{String, Nothing}

Return the URL of an `item`, if it has one.
"""
url(item::Item) = get(iteminfo(item), :url, nothing)

end

# 001 - Repository Skeleton

Create the minimal .NET repository structure for Kyft.

## Scope

- Add a solution file.
- Add a `src/Kyft` class library targeting .NET 10.
- Enable nullable reference types and implicit usings.
- Add a minimal package description.
- Add a root ignore file for common .NET build outputs.

## Acceptance

- `dotnet build` succeeds.
- No public Kyft API is designed beyond the default project placeholder.
- The repository can be restored and built from a clean checkout.

## Out Of Scope

- Test project setup.
- Public builder APIs.
- Runtime window behavior.

